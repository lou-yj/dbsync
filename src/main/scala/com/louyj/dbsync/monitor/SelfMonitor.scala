package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.dbsync.sync.ComponentManager
import com.louyj.dbsync.sync.ComponentStatus.{GREEN, RED, YELLOW}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import java.util.{Timer, TimerTask}

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/27
 *
 */
class SelfMonitor(componentManager: ComponentManager, ctx: SystemContext)
  extends TimerTask {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  new Endpoints(ctx.app, componentManager, ctx)
  val timer = new Timer(true)
  val scheduleInterval = TimeUnit.SECONDS.toMillis(10)
  timer.schedule(this, scheduleInterval, scheduleInterval)

  override def run(): Unit = {
    try {
      doRun()
    } catch {
      case e: Throwable => logger.error("", e)
    }
  }

  def doRun(): Unit = {
    var ctxStatus = GREEN
    if (componentManager.components.values.filter(_.componentStatus() == RED).nonEmpty) {
      ctxStatus = RED
    } else if (componentManager.components.values.filter(_.componentStatus() == YELLOW).nonEmpty) {
      ctxStatus = YELLOW
    }
    ctx.componentStatus = ctxStatus
    val syncState = ctx.dbConfigs.map(dbConfig => {
      val jdbc = ctx.dsPools.jdbcTemplate(dbConfig.name)
      val dbOpt = dbOpts(dbConfig.`type`)
      dbOpt.syncState(dbConfig, jdbc)
    })
    ctx.syncStatus = syncState.reduce((v1, v2) =>
      SyncState(
        "N/A",
        v1.pending + v2.pending,
        v1.blocked + v2.blocked,
        v1.error + v2.error,
        v1.success + v2.success,
        v1.others + v2.others)
    )
    if (ctx.sysConfig.restartWhenRedStatus && ctx.componentStatus == RED) {
      logger.warn("Component status in RED status, restarting ...")
      val notGreenComponents = componentManager.format(componentManager.components.filter(_._2.componentStatus() != GREEN))
      logger.warn(s"Component status detail ${jackson.writeValueAsString(notGreenComponents)}")
      val redNames = componentManager.components.values.filter(_.componentStatus() == RED).map(_.getName).toList
      ctx.restart(s"Component ${redNames.mkString(",")} in RED status")
    } else if (ctx.syncStatus.error > ctx.sysConfig.restartWhenErrorOver) {
      logger.warn(s"Sync error count ${ctx.syncStatus.error} over ${ctx.sysConfig.restartWhenErrorOver}, restarting ...")
      logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
      ctx.restart(s"Sync error count ${ctx.syncStatus.error} over ${ctx.sysConfig.restartWhenErrorOver}")
    } else if (ctx.syncStatus.blocked > ctx.sysConfig.restartWhenBlockedOver) {
      logger.warn(s"Sync blocked count ${ctx.syncStatus.blocked} over ${ctx.sysConfig.restartWhenBlockedOver}, restarting ...")
      logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
      ctx.restart(s"Sync blocked count ${ctx.syncStatus.blocked} over ${ctx.sysConfig.restartWhenBlockedOver}")
    }
  }

  def destroy() = {
    timer.cancel()
  }

}
