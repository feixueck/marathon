package mesosphere.marathon.core.election

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.event.EventStream
import akka.pattern.after
import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.event.LocalLeadershipEvent
import mesosphere.marathon.metrics.Metrics
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class PseudoElectionService(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    metrics: Metrics = new Metrics(new MetricRegistry),
    electionCallbacks: Seq[ElectionCallback] = Seq.empty,
    delegate: ElectionDelegate
) extends ElectionServiceBase(config, system, eventStream, metrics, electionCallbacks, delegate) {
  private val log = LoggerFactory.getLogger(getClass.getName)

  override def offerLeadershipImpl(): Unit = synchronized {
     log.info("Not using HA and therefore electing as leader by default")
     startLeadership(_ => stopLeadershop())
  }
}
