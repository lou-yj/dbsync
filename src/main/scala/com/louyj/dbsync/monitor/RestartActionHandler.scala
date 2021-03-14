package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import org.slf4j.LoggerFactory

import scala.collection.mutable

class RestartActionHandler extends ActionHandler {

  val logger = LoggerFactory.getLogger(getClass)

  override def name(): String = "restart"

  override def doAction(monitorConfig: MonitorConfig, ctx: SystemContext, components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String): Unit = {
    ctx.restart(reason)
  }

}
