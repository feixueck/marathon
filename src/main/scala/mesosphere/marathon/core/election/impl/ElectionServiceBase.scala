package mesosphere.marathon.core.election.impl

import akka.actor.ActorSystem
import akka.event.EventStream
import akka.pattern.after
import com.codahale.metrics.{ Gauge, MetricRegistry }
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.election.{ ElectionCallback, ElectionCandidate, ElectionService }
import mesosphere.marathon.event.LocalLeadershipEvent
import mesosphere.marathon.metrics.Metrics
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NonFatal

private[impl] object ElectionServiceBase {
  protected type Abdicator = /* error: */ Boolean => Unit

  abstract class State {
    def getCandidate(): Option[ElectionCandidate] = this match {
      case Idle(c)             => c
      case Leading(c, _)       => Some(c)
      case Abdicating(c, _, _) => Some(c)
      case Offering(c)         => Some(c)
      case Offered(c)          => Some(c)
    }
  }

  case class Idle(candidate: Option[ElectionCandidate]) extends State
  case class Leading(candidate: ElectionCandidate, abdicate: Abdicator) extends State
  case class Abdicating(candidate: ElectionCandidate, reoffer: Boolean, shortcut: Boolean = false) extends State
  case class Offering(candidate: ElectionCandidate) extends State
  case class Offered(candidate: ElectionCandidate) extends State
}

abstract class ElectionServiceBase(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    metrics: Metrics = new Metrics(new MetricRegistry),
    electionCallbacks: Seq[ElectionCallback] = Seq.empty,
    backoff: Backoff) extends ElectionService {
  import ElectionServiceBase._

  private lazy val log = LoggerFactory.getLogger(getClass.getName)

  private[impl] var state: State = Idle(candidate = None)

  import scala.concurrent.ExecutionContext.Implicits.global

  override def isLeader: Boolean = synchronized {
    state match {
      case Leading(_, _)       => true
      case Abdicating(_, _, _) => true
      case _                   => false
    }
  }

  override def abdicateLeadership(error: Boolean = false, reoffer: Boolean = false): Unit = synchronized {
    state match {
      case Leading(candidate, abdicate) =>
        log.info("Abdicating leadership while leading")
        state = Abdicating(candidate, reoffer)
        abdicate(error)
      case Abdicating(candidate, alreadyReoffering, shortcut) =>
        log.info("Abdicating leadership while already in process of abdicating")
        state = Abdicating(candidate, alreadyReoffering || reoffer, shortcut)
      case Offering(candidate) =>
        log.info("Canceling leadership offer waiting for backoff")
        state = Abdicating(candidate, reoffer)
      case Offered(candidate) =>
        log.info("Abdicating leadership while candidating")
        state = Abdicating(candidate, reoffer)
      case Idle(candidate) =>
        log.info("Abdicating leadership while being NO candidate")
        if (reoffer) {
          candidate match {
            case None    => log.error("Cannot reoffer leadership without being a leadership candidate")
            case Some(c) => offerLeadership(c)
          }
        }
    }
  }

  protected def offerLeadershipImpl(): Unit

  private def setOfferState(offeringCase: => Unit, idleCase: => Unit): Unit = synchronized {
    state match {
      case Abdicating(candidate, reoffer, shortcut) =>
        log.error("Will reoffer leadership after abdicating")
        state = Abdicating(candidate, reoffer = true, shortcut)
      case Leading(candidate, abdicate) =>
        log.info("Ignoring leadership offer while being leader")
      case Offering(candidate) =>
        offeringCase
      case Offered(candidate) =>
        log.info("Ignoring repeated leadership offer")
      case Idle(candidate) =>
        idleCase
    }
  }

  override def offerLeadership(candidate: ElectionCandidate): Unit = synchronized {
    log.info(s"Will offer leadership after ${backoff.value()} backoff")
    setOfferState({
      // some offering attempt is running
      log.info("Ignoring repeated leadership offer")
    }, {
      // backoff idle case
      state = Offering(candidate)
      after(backoff.value(), system.scheduler)(Future {
        synchronized {
          setOfferState({
            // now after backoff actually set Offered state
            state = Offered(candidate)
            offerLeadershipImpl()
          }, {
            // state became Idle meanwhile
            log.info("Canceling leadership offer attempt")
          })
        }
      })
    })
  }

  protected def stopLeadership(): Unit = synchronized {
    val (candidate, reoffer, shortcut) = state match {
      case Leading(c, a)         => (c, false, false)
      case Abdicating(c, ro, sc) => (c, ro, sc)
      case Offered(c)            => (c, false, false)
      case Offering(c)           => (c, false, false)
      case Idle(c)               => (c.get, false, false)
    }
    state = Idle(Some(candidate))

    if (!shortcut) {
      log.info(s"Call onDefeated leadership callbacks on ${electionCallbacks.mkString(", ")}")
      Await.result(Future.sequence(electionCallbacks.map(_.onDefeated)), config.zkTimeoutDuration)
      log.info(s"Finished onDefeated leadership callbacks")

      // Our leadership has been defeated. Tell the candidate and the world
      candidate.stopLeadership()
      eventStream.publish(LocalLeadershipEvent.Standby)
      stopMetrics()
    }

    // call abdiction continuations
    if (reoffer) {
      offerLeadership(candidate)
    }
  }

  protected def startLeadership(abdicate: Abdicator): Unit = synchronized {
    def backoffAbdicate(error: Boolean) = {
      if (error) backoff.increase()
      abdicate(error)
    }

    state match {
      case Abdicating(candidate, reoffer, _) =>
        log.info("Became leader and abdicating immediately")
        state = Abdicating(candidate, reoffer, shortcut = true)
        abdicate
      case _ =>
        val candidate = state.getCandidate().get // Idle(None) is not possible
        state = Leading(candidate, backoffAbdicate)
        try {
          // Start the leader duration metric
          startMetrics()

          // run all leadership callbacks
          log.info(s"""Call onElected leadership callbacks on ${electionCallbacks.mkString(", ")}""")
          Await.result(Future.sequence(electionCallbacks.map(_.onElected)), config.onElectedPrepareTimeout().millis)
          log.info(s"Finished onElected leadership callbacks")

          candidate.startLeadership()

          // tell the world about us
          eventStream.publish(LocalLeadershipEvent.ElectedAsLeader)

          // We successfully took over leadership. Time to reset backoff
          if (isLeader) {
            backoff.reset()
          }
        }
        catch {
          case NonFatal(e) => // catch Scala and Java exceptions
            log.error("Failed to take over leadership", e)
            abdicateLeadership(error = true)
        }
    }
  }

  private def startMetrics(): Unit = {
    metrics.gauge("service.mesosphere.marathon.leaderDuration", new Gauge[Long] {
      val startedAt = System.currentTimeMillis()

      override def getValue: Long = {
        System.currentTimeMillis() - startedAt
      }
    })
  }

  private def stopMetrics(): Unit = {
    metrics.registry.remove("service.mesosphere.marathon.leaderDuration")
  }
}
