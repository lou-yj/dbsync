package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import org.slf4j.LoggerFactory

import scala.collection.mutable

class RestartActionHandler extends ActionHandler {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  override def name(): String = "restart"

  override def doAction(monitorConfig: MonitorConfig, ctx: SystemContext, components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String): Unit = {
    ctx.restart(reason)
  }

}
