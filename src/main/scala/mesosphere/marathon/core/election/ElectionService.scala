package mesosphere.marathon.core.election

import scala.concurrent.Future

trait LeadershipAbdication {
  def abdicateLeadership(error: Boolean = false): Unit
}

trait ElectionService extends LeadershipAbdication {
  def isLeader: Boolean
  def offerLeadership(): Unit
}

trait ElectionDelegate {
  def defeatLeadership(): Unit
  def electLeadership(abdicate: ElectionService.Abdicator): Unit
}

trait ElectionCallback {
  /**
   * Will get called _before_ the scheduler driver is started.
   */
  def onElected: Future[Unit]

  /**
   * Will get called after leadership is abdicated.
   */
  def onDefeated: Future[Unit]
}

object ElectionService {
  type Abdicator = /* error: */ Boolean => Unit
}
