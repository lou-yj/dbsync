package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext

import scala.collection.mutable

trait ActionHandler {

  def name(): String

  def doAction(ctx: SystemContext, components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String, alarmArgs: Map[String, Object]): Unit

}