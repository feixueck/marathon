package mesosphere.marathon.core.election

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.event.EventStream
import akka.pattern.after
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.event.LocalLeadershipEvent
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

private class PseudoElectionService(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    delegate: ElectionDelegate
) extends ElectionService {
  private val log = LoggerFactory.getLogger(getClass.getName)

  // If running in single scheduler mode, this node is the leader in the beginning.
  protected val leader = new AtomicBoolean(true)
  override def isLeader: Boolean = leader.get()

  override def offerLeadership(): Unit = after(0.seconds, system.scheduler)(Future {
    synchronized {
      log.info("Not using HA and therefore electing as leader by default")
      leader.set(true)
      eventStream.publish(LocalLeadershipEvent.ElectedAsLeader)
      delegate.electLeadership(_ => abdicateLeadership())
    }
  })

  override def abdicateLeadership(error: Boolean): Unit = synchronized {
    if (leader.compareAndSet(true, false)) {
      log.info(s"Abdicating with error=$error")
      delegate.defeatLeadership()
    }
  }
}
