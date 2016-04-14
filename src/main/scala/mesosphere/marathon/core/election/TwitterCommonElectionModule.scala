package mesosphere.marathon.core.election

import java.util.concurrent.atomic.AtomicBoolean

import mesosphere.marathon.MarathonConf

class TwitterCommonElectionModule(
    conf: MarathonConf) extends ElectionService {
  // If running in single scheduler mode, this node is the leader.
  private val leader = new AtomicBoolean(!conf.highlyAvailable())

  override def isLeader: Boolean = leader.get()
}
