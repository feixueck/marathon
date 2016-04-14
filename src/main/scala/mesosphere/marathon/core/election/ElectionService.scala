package mesosphere.marathon.core.election

import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Named

import akka.actor.ActorSystem
import akka.event.EventStream
import akka.pattern.{ after, ask }
import com.codahale.metrics.{MetricRegistry, Gauge}
import com.google.inject.{Singleton, Provides}
import com.twitter.common.base.{Supplier, ExceptionalCommand}
import com.twitter.common.zookeeper.Candidate.Leader
import com.twitter.common.zookeeper.Group.JoinException
import com.twitter.common.zookeeper.{Group, CandidateImpl, Candidate, ZooKeeperClient}
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.{MarathonConf, LeadershipCallback}
import mesosphere.marathon.event.{EventModule, LocalLeadershipEvent}
import mesosphere.marathon.metrics.Metrics
import org.apache.zookeeper.ZooDefs
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait ElectionService {
  def isLeader: Boolean
  def offerLeadership(): Unit
}

trait ElectionDelegate {
  def defeatLeadership(): Unit
  def electLeadership(abdicate: ElectionService.Abdicator): Unit
}

object ElectionService {
  type Abdicator = /* error: */ Boolean => Unit
}

private abstract class ElectionServiceBase(
  config: MarathonConf
) extends ElectionService {
  // If running in single scheduler mode, this node is the leader.
  protected val leader = new AtomicBoolean(!config.highlyAvailable())

  override def isLeader: Boolean = leader.get()
}

private class PseudoElectionLogic(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    delegate: ElectionDelegate
) extends ElectionServiceBase(config) {
  private val log = LoggerFactory.getLogger(getClass.getName)

  override def offerLeadership(): Unit = after(0.seconds, system.scheduler)(Future {
    synchronized {
      log.info("Not using HA and therefore electing as leader by default")
      leader.set(true)
      eventStream.publish(LocalLeadershipEvent.ElectedAsLeader)
      delegate.electLeadership(_ => {
        log.info("Abdicating")
        leader.set(false)
        delegate.defeatLeadership()
      })
    }
  })
}

private class ExponentialBackoff(
    initialValue: FiniteDuration = 0.5.seconds,
    maximumValue: FiniteDuration = 16.seconds,
    exponent: Double = 2,
    name: String = "unnamed"
) {
  private val log = LoggerFactory.getLogger(getClass.getName)
  private var v = initialValue

  def value(): FiniteDuration = v

  def increase(): Unit = synchronized {
    if (v <= maximumValue) {
      v *= exponent
      log.info(s"Increasing $name backoff to $v")
    }
  }

  def reset(): Unit = synchronized {
    log.info(s"Reset $name backoff")
    v = initialValue
  }
}

class TwitterCommonsElectionService(
    delegate: ElectionDelegate,
    eventStream: EventStream,
    config: MarathonConf,
    http: HttpConf,
    leadershipCallbacks: Seq[LeadershipCallback] = Seq.empty,
    metrics: Metrics = new Metrics(new MetricRegistry),
    system: ActorSystem,
    hostPort: String,
    zk: ZooKeeperClient
) extends ElectionServiceBase(config) with Leader {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)
  private lazy val backoff = new ExponentialBackoff(name="offerLeadership")

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

  private lazy val candidate = provideCandidate(zk)

  override def offerLeadership(): Unit = synchronized {
    log.info(s"Will offer leadership after ${backoff.value()} backoff")
    after(backoff.value(), system.scheduler)(Future {
      candidate.synchronized {
        log.info("Using HA and therefore offering leadership")
        candidate.offerLeadership(this)
      }
    })
  }

  //Begin Leader interface, which is required for CandidateImpl.
  override def onDefeated(): Unit = synchronized {
    log.info("Defeated (Leader Interface)")

    log.info(s"Call onDefeated leadership callbacks on ${leadershipCallbacks.mkString(", ")}")
    Await.result(Future.sequence(leadershipCallbacks.map(_.onDefeated)), config.zkTimeoutDuration)
    log.info(s"Finished onDefeated leadership callbacks")

    eventStream.publish(LocalLeadershipEvent.Standby)

    leader.set(false)

    // Our leadership has been defeated and thus we call the defeatLeadership() method.
    delegate.defeatLeadership()

    stopMetrics()
  }

  override def onElected(abdicateCmd: ExceptionalCommand[JoinException]): Unit = synchronized {
    val abdicate: ElectionService.Abdicator = error => {
      if (error) backoff.increase()
      abdicateCmd.execute()
      // defeatLeadership() is called in onDefeated
    }

    try {
      log.info("Elected (Leader Interface)")
      eventStream.publish(LocalLeadershipEvent.ElectedAsLeader)

      // Start the leader duration metric
      startMetrics()

      // run all leadership callbacks
      log.info(s"""Call onElected leadership callbacks on ${leadershipCallbacks.mkString(", ")}""")
      Await.result(Future.sequence(leadershipCallbacks.map(_.onElected)), config.onElectedPrepareTimeout().millis)
      log.info(s"Finished onElected leadership callbacks")

      // We have been elected. Thus, elect leadership with the abdication command.
      leader.set(true)
      delegate.electLeadership(abdicate)

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
  //End Leader interface

  def startMetrics(): Unit = {
    metrics.gauge("service.mesosphere.marathon.leaderDuration", new Gauge[Long] {
      val startedAt = System.currentTimeMillis()

      override def getValue: Long = {
        System.currentTimeMillis() - startedAt
      }
    })
  }

  def stopMetrics(): Unit = {
    metrics.registry.remove("service.mesosphere.marathon.leaderDuration")
  }
}