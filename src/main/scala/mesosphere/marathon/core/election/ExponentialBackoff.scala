package mesosphere.marathon.core.election

import org.slf4j.LoggerFactory

import scala.concurrent.duration._

private class ExponentialBackoff(
    initialValue: FiniteDuration = 0.5.seconds,
    maximumValue: FiniteDuration = 16.seconds,
    name: String = "unnamed") {
  private val log = LoggerFactory.getLogger(getClass.getName)
  private var v = initialValue

  def value(): FiniteDuration = v

  def increase(): Unit = synchronized {
    if (v <= maximumValue) {
      v *= 2
      log.info(s"Increasing $name backoff to $v")
    }
  }

  def reset(): Unit = synchronized {
    log.info(s"Reset $name backoff")
    v = initialValue
  }
}
