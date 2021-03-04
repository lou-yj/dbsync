package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.sync.HeartbeatComponent
import org.slf4j.LoggerFactory

class SimpleAlarmSender extends AlarmSender {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  override def name(): String = "simple"

  override def sendAlarm(components: Iterable[HeartbeatComponent], syncStatus: SyncState, reason: String, alarmArgs: Map[String, Object]): Unit = {
    if (components != null && components.size > 0)
      logger.warn(s"Component status detail ${jackson.writeValueAsString(components)}")
    if (syncStatus != null) {
      logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncStatus)}")
    }
    logger.warn(s"Alarm send, reason $reason")
  }

}
