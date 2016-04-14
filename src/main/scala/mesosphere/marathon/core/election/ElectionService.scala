package mesosphere.marathon.core.election

trait ElectionService {
  def isLeader: Boolean
}
