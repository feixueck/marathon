package mesosphere.marathon.core.election.impl

import akka.actor.ActorSystem
import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import com.twitter.common.zookeeper.ZooKeeperClient
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.election.{ ElectionCallback, ElectionCandidate }
import mesosphere.marathon.metrics.Metrics
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

class CuratorElectionService(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    http: HttpConf,
    metrics: Metrics = new Metrics(new MetricRegistry),
    hostPort: String,
    zk: ZooKeeperClient,
    electionCallbacks: Seq[ElectionCallback] = Seq.empty,
    delegate: ElectionCandidate,
    backoff: ExponentialBackoff
) extends ElectionServiceBase(
  config, system, eventStream, metrics, electionCallbacks, delegate, backoff
) with LeaderLatchListener {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)

  private lazy val maxZookeeperBackoffTime = 1000 * 300
  private lazy val minZookeeperBackoffTime = 500

  private lazy val latch = new LeaderLatch(provideCuratorClient(zk), "curator-leader", hostPort)

  override def leaderHostPort: Option[String] = synchronized {
    val l = latch.getLeader
    if (l.isLeader) Some(l.getId) else None
  }

  override def offerLeadershipImpl(): Unit = synchronized {
    log.info("Using HA and therefore offering leadership")
    latch.addListener(this) // idem-potent
    latch.start()
  }

  //Begin LeaderLatchListener interface
  override def notLeader(): Unit = synchronized {
    log.info("Defeated (Leader Interface)")
    stopLeadership()
  }

  override def isLeader(): Unit = synchronized {
    log.info("Elected (Leader Interface)")
    startLeadership(error => synchronized {
      latch.close()
      // stopLeadership() is called in notLeader
    })
  }
  //End Leader interface

  private def provideCuratorClient(zk: ZooKeeperClient): CuratorFramework = {
    val client = CuratorFrameworkFactory.newClient(zk.getConnectString,
      new ExponentialBackoffRetry(minZookeeperBackoffTime, maxZookeeperBackoffTime))
    client.start()
    client.getZookeeperClient.blockUntilConnectedOrTimedOut()
    client
  }
}
