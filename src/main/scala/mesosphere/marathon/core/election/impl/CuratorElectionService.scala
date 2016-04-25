package mesosphere.marathon.core.election.impl

import akka.actor.ActorSystem
import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import com.twitter.common.zookeeper.ZooKeeperClient
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.election.ElectionCallback
import mesosphere.marathon.metrics.Metrics
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{ ZooDefs, KeeperException, CreateMode }
import org.slf4j.LoggerFactory

class CuratorElectionService(
  config: MarathonConf,
  system: ActorSystem,
  eventStream: EventStream,
  http: HttpConf,
  metrics: Metrics = new Metrics(new MetricRegistry),
  hostPort: String,
  zk: ZooKeeperClient,
  electionCallbacks: Seq[ElectionCallback] = Seq.empty,
  backoff: ExponentialBackoff) extends ElectionServiceBase(
  config, system, eventStream, metrics, electionCallbacks, backoff
) {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)

  private lazy val maxZookeeperBackoffTime = 1000 * 300
  private lazy val minZookeeperBackoffTime = 500

  private lazy val latch = new LeaderLatch(provideCuratorClient(zk), config.zooKeeperLeaderPath + "-curator", hostPort)

  override def leaderHostPort: Option[String] = synchronized {
    val l = latch.getLeader
    if (l.isLeader) Some(l.getId) else None
  }

  override def offerLeadershipImpl(): Unit = synchronized {
    log.info("Using HA and therefore offering leadership")
    latch.addListener(Listener) // idem-potent
    latch.start()
  }

  private object Listener extends LeaderLatchListener {
    override def notLeader(): Unit = synchronized {
      log.info("Defeated (Leader Interface)")
      stopLeadership()

      // remove tombstone for twitter commons
      twitterCommons.deleteTombstone(onlyMyself = true)
    }

    override def isLeader(): Unit = synchronized {
      log.info("Elected (Leader Interface)")
      startLeadership(error => synchronized {
        latch.close()
        // stopLeadership() is called in notLeader
      })

      // write a tombstone into the old twitter commons leadership election path which always
      // wins the selection there.
      twitterCommons.createTombstone()
    }
  }

  private def provideCuratorClient(zk: ZooKeeperClient): CuratorFramework = {
    val client = CuratorFrameworkFactory.newClient(zk.getConnectString,
      new ExponentialBackoffRetry(minZookeeperBackoffTime, maxZookeeperBackoffTime))
    client.start()
    client.getZookeeperClient.blockUntilConnectedOrTimedOut()
    client
  }

  private object twitterCommons {
    lazy val group = TwitterCommonsElectionService.group(zk, config)
    lazy val tombstoneMemberName = "member_-00000000" // - precedes 0-9 in ASCII
    lazy val tombstonePath = group.getMemberPath(tombstoneMemberName)

    def createTombstone(): Unit = {
      try {
        twitterCommons.deleteTombstone()
        val data = hostPort.getBytes("UTF-8")
        zk.get.create(twitterCommons.tombstonePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      }
      catch {
        case e: Exception =>
          log.error(s"Exception while creating tombstone for twitter commons leader election: ${e.getMessage}")
          abdicateLeadership(error = true)
      }
    }

    def deleteTombstone(onlyMyself: Boolean = false): Unit = {
      val tombstone = Option(zk.get.exists(tombstonePath, false))
      tombstone match {
        case None =>
        case Some(ts) =>
          try {
            if (!onlyMyself || group.getMemberData(tombstoneMemberName).toString == hostPort) {
              zk.get.delete(tombstonePath, ts.getVersion)
            }
          }
          catch {
            case _: KeeperException.NoNodeException     =>
            case _: KeeperException.BadVersionException =>
          }
      }
    }
  }
}
