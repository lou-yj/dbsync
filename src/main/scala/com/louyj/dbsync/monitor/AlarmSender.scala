package com.louyj.dbsync.monitor

import com.louyj.dbsync.sync.HeartbeatComponent

trait AlarmSender {

  def name(): String

  def sendAlarm(components: Iterable[HeartbeatComponent], syncStatus: SyncState, reason: String, alarmArgs: Map[String, Object]): Unit

}