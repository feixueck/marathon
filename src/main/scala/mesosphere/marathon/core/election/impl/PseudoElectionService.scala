package mesosphere.marathon.core.election.impl

import akka.actor.ActorSystem
import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.election.{ ElectionCallback, ElectionCandidate }
import mesosphere.marathon.metrics.Metrics
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

class PseudoElectionService(
  config: MarathonConf,
  system: ActorSystem,
  eventStream: EventStream,
  metrics: Metrics = new Metrics(new MetricRegistry),
  hostPort: String,
  electionCallbacks: Seq[ElectionCallback] = Seq.empty,
  delegate: ElectionCandidate,
  backoff: ExponentialBackoff
) extends ElectionServiceBase(
  config, system, eventStream, metrics, electionCallbacks, delegate, backoff
) {
  private val log = LoggerFactory.getLogger(getClass.getName)

  def leaderHostPort: Option[String] = if (isLeader) Some(hostPort) else None

  override def offerLeadershipImpl(): Unit = synchronized {
    log.info("Not using HA and therefore electing as leader by default")
    startLeadership(_ => stopLeadership())
  }
}
