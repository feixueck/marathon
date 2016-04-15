package mesosphere.marathon.core.election

import akka.actor.ActorSystem
import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import com.twitter.common.base.{ Supplier, ExceptionalCommand }
import com.twitter.common.zookeeper.Candidate.Leader
import com.twitter.common.zookeeper.Group.JoinException
import com.twitter.common.zookeeper.{ Group, CandidateImpl, Candidate, ZooKeeperClient }
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.metrics.Metrics
import org.apache.zookeeper.ZooDefs
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

class TwitterCommonsElectionService(
  config: MarathonConf,
  system: ActorSystem,
  eventStream: EventStream,
  http: HttpConf,
  metrics: Metrics = new Metrics(new MetricRegistry),
  hostPort: String,
  zk: ZooKeeperClient,
  electionCallbacks: Seq[ElectionCallback] = Seq.empty,
  delegate: ElectionDelegate) extends ElectionServiceBase(
  config, system, eventStream, metrics, electionCallbacks, delegate
) with Leader {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)
  private lazy val candidate = provideCandidate(zk)

  override def offerLeadershipImpl(): Unit = candidate.synchronized {
    log.info("Using HA and therefore offering leadership")
    candidate.offerLeadership(this)
  }

  //Begin Leader interface, which is required for CandidateImpl.
  override def onDefeated(): Unit = synchronized {
    log.info("Defeated (Leader Interface)")
    stopLeadershop()
  }

  override def onElected(abdicateCmd: ExceptionalCommand[JoinException]): Unit = synchronized {
    log.info("Elected (Leader Interface)")
    startLeadership(error => {
      abdicateCmd.execute()
      // stopLeadership() is called in onDefeated
    })
  }
  //End Leader interface

  def provideCandidate(zk: ZooKeeperClient): Candidate = {
    log.info("Registering in ZooKeeper with hostPort:" + hostPort)
    new CandidateImpl(new Group(zk, ZooDefs.Ids.OPEN_ACL_UNSAFE, config.zooKeeperLeaderPath),
      new Supplier[Array[Byte]] {
        def get(): Array[Byte] = {
          hostPort.getBytes("UTF-8")
        }
      }
    )
  }
}
