package mesosphere.marathon.core.election

import akka.actor.ActorSystem
import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.MarathonConf
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
  delegate: ElectionDelegate) extends ElectionServiceBase(
  config, system, eventStream, metrics, electionCallbacks, delegate
) {
  private val log = LoggerFactory.getLogger(getClass.getName)

  def leader: Option[String] = if (isLeader) Some(hostPort) else None

  override def offerLeadershipImpl(): Unit = synchronized {
    log.info("Not using HA and therefore electing as leader by default")
    startLeadership(_ => stopLeadershop())
  }
}