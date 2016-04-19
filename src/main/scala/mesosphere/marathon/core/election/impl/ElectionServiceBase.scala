package mesosphere.marathon.core.election.impl

import java.util.concurrent.atomic.AtomicReference

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

abstract class ElectionServiceBase(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    metrics: Metrics = new Metrics(new MetricRegistry),
    electionCallbacks: Seq[ElectionCallback] = Seq.empty,
    delegate: ElectionCandidate) extends ElectionService {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)
  private lazy val backoff = new ExponentialBackoff(name = "offerLeadership")
  protected type Abdicator = /* error: */ Boolean => Unit
  private val abdicate = new AtomicReference[Option[Abdicator]](None)

  import scala.concurrent.ExecutionContext.Implicits.global

  override def isLeader: Boolean = abdicate.get().isDefined

  override def abdicateLeadership(error: Boolean): Unit = synchronized {
    val abdicate = ElectionServiceBase.this.abdicate.getAndSet(None)
    abdicate.foreach(_.apply(error))
  }

  protected def offerLeadershipImpl(): Unit

  override def offerLeadership(): Unit = synchronized {
    log.info(s"Will offer leadership after ${backoff.value()} backoff")
    after(backoff.value(), system.scheduler)(Future {
      offerLeadershipImpl()
    })
  }

  protected def stopLeadership(): Unit = synchronized {
    abdicate.set(None)

    log.info(s"Call onDefeated leadership callbacks on ${electionCallbacks.mkString(", ")}")
    Await.result(Future.sequence(electionCallbacks.map(_.onDefeated)), config.zkTimeoutDuration)
    log.info(s"Finished onDefeated leadership callbacks")

    // Our leadership has been defeated and thus we call the defeatLeadership() method.
    delegate.stopLeadership()

    // tell the world about us
    eventStream.publish(LocalLeadershipEvent.Standby)

    stopMetrics()
  }

  protected def startLeadership(abdicate: Abdicator): Unit = synchronized {
    def backoffAbdicate(error: Boolean) = {
      if (error) backoff.increase()
      abdicate(error)
    }

    try {
      log.info("Elected (Leader Interface)")

      // We have been elected. Thus, elect leadership with the abdication command.
      ElectionServiceBase.this.abdicate.set(Some(backoffAbdicate))

      // Start the leader duration metric
      startMetrics()

      // run all leadership callbacks
      log.info(s"""Call onElected leadership callbacks on ${electionCallbacks.mkString(", ")}""")
      Await.result(Future.sequence(electionCallbacks.map(_.onElected)), config.onElectedPrepareTimeout().millis)
      log.info(s"Finished onElected leadership callbacks")

      delegate.startLeadership()

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
        abdicate(true) // error=true
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
