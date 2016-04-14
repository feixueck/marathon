package mesosphere.marathon.core.election

import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

import akka.actor.ActorSystem
import akka.event.EventStream
import akka.pattern.after
import com.codahale.metrics.{MetricRegistry, Gauge}
import com.twitter.common.base.{Supplier, ExceptionalCommand}
import com.twitter.common.zookeeper.Candidate.Leader
import com.twitter.common.zookeeper.Group.JoinException
import com.twitter.common.zookeeper.{Group, CandidateImpl, Candidate, ZooKeeperClient}
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.event.LocalLeadershipEvent
import mesosphere.marathon.metrics.Metrics
import org.apache.zookeeper.ZooDefs
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

class TwitterCommonsElectionService(
    delegate: ElectionDelegate,
    eventStream: EventStream,
    config: MarathonConf,
    http: HttpConf,
    leadershipCallbacks: Seq[ElectionCallback] = Seq.empty,
    metrics: Metrics = new Metrics(new MetricRegistry),
    system: ActorSystem,
    hostPort: String,
    zk: ZooKeeperClient
) extends ElectionService with Leader {
  private lazy val log = LoggerFactory.getLogger(getClass.getName)
  private lazy val backoff = new ExponentialBackoff(name="offerLeadership")
  private lazy val candidate = provideCandidate(zk)
  private val abdicate = new AtomicReference[Option[ElectionService.Abdicator]](None)

  override def isLeader: Boolean = abdicate.get().isDefined

  override def abdicateLeadership(error: Boolean): Unit = synchronized {
    val abdicate = TwitterCommonsElectionService.this.abdicate.getAndSet(None)
    abdicate.foreach(_.apply(error))
  }

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

    abdicate.set(None)

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
      TwitterCommonsElectionService.this.abdicate.set(Some(abdicate))
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