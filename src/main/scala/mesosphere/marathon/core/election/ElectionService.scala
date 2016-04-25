package mesosphere.marathon.core.election

import scala.concurrent.Future

trait LeadershipAbdication {
  def abdicateLeadership(error: Boolean = false): Unit
}

trait ElectionService extends LeadershipAbdication {
  def isLeader: Boolean
  def leaderHostPort: Option[String]
  def offerLeadership(): Unit
}

trait ElectionCandidate {
  def stopLeadership(): Unit
  def startLeadership(): Unit
}

trait ElectionCallback {
  /**
    * Will get called _before_ the ElectionCandidate (usually the scheduler driver) starts leadership.
    */
  def onElected: Future[Unit]

  /**
    * Will get called after leadership is abdicated.
    */
  def onDefeated: Future[Unit]
}
