package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig

import scala.collection.mutable

trait ActionHandler {

  def name(): String

  def doAction(monitorConfig: MonitorConfig, ctx: SystemContext, components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String): Unit

}