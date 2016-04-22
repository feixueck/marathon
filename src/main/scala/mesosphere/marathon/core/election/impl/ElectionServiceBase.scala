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

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NonFatal

private[impl] object ElectionServiceBase {
  protected type Abdicator = /* error: */ Boolean => Unit

  abstract class State
  case class Idle() extends State
  case class Leading(abdicate: Abdicator) extends State
  case class Abdicating(reoffer: Boolean, shortcut: Boolean = false) extends State
  case class Offering() extends State
  case class Offered() extends State
}

abstract class ElectionServiceBase(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    metrics: Metrics = new Metrics(new MetricRegistry),
    electionCallbacks: Seq[ElectionCallback] = Seq.empty,
    candidate: ElectionCandidate,
    backoff: Backoff) extends ElectionService {
  import ElectionServiceBase._

  private lazy val log = LoggerFactory.getLogger(getClass.getName)

  private[impl] var state: State = Idle()

  import scala.concurrent.ExecutionContext.Implicits.global

  override def isLeader: Boolean = synchronized {
    state match {
      case Leading(_)       => true
      case Abdicating(_, _) => true
      case _                => false
    }
  }

  override def abdicateLeadership(error: Boolean = false, reoffer: Boolean = false): Unit = synchronized {
    state match {
      case Leading(abdicate) =>
        log.info("Abdicating leadership while leading")
        state = Abdicating(reoffer)
        abdicate(error)
      case Abdicating(alreadyReoffering, shortcut) =>
        log.info("Abdicating leadership while already in process of abdicating")
        state = Abdicating(alreadyReoffering || reoffer, shortcut)
      case Offering() =>
        log.info("Canceling leadership offer waiting for backoff")
        state = Abdicating(reoffer)
      case Offered() =>
        log.info("Abdicating leadership while candidating")
        state = Abdicating(reoffer)
      case Idle() =>
        log.info("Abdicating leadership while being NO candidate")
        if (reoffer) {
          offerLeadership()
        }
    }
  }

  protected def offerLeadershipImpl(): Unit

  private def setOfferState(offeringCase: => Unit, idleCase: => Unit): Unit = synchronized {
    state match {
      case Abdicating(reoffer, shortcut) =>
        log.error("Will reoffer leadership after abdicating")
        state = Abdicating(reoffer = true, shortcut)
      case Leading(abdicate) =>
        log.info("Ignoring leadership offer while being leader")
      case Offering() =>
        offeringCase
      case Offered() =>
        log.info("Ignoring repeated leadership offer")
      case Idle() =>
        idleCase
    }
  }

  override def offerLeadership(): Unit = synchronized {
    log.info(s"Will offer leadership after ${backoff.value()} backoff")
    setOfferState({
      // some offering attempt is running
      log.info("Ignoring repeated leadership offer")
    }, {
      // backoff idle case
      state = Offering()
      after(backoff.value(), system.scheduler)(Future {
        synchronized {
          setOfferState({
            // now after backoff actually set Offered state
            state = Offered()
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
    val (reoffer, shortcut) = state match {
      case Abdicating(ro, sc) => (ro, sc)
      case _                  => (false, false)
    }
    state = Idle()

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
      offerLeadership()
    }
  }

  protected def startLeadership(abdicate: Abdicator): Unit = synchronized {
    def backoffAbdicate(error: Boolean) = {
      if (error) backoff.increase()
      abdicate(error)
    }

    state match {
      case Abdicating(reoffer, _) =>
        log.info("Became leader and abdicating immediately")
        state = Abdicating(reoffer, shortcut = true)
        abdicate
      case _ =>
        state = Leading(backoffAbdicate)
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
