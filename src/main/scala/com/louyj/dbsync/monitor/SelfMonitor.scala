package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.dbsync.sync.ComponentStatus.{GREEN, RED, YELLOW}
import com.louyj.dbsync.sync.{ComponentManager, HeartbeatComponent}
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
  val scheduleInterval = TimeUnit.SECONDS.toMillis(60)
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
    if (ctx.monitorConfig != null) {
      ctx.monitorConfig.foreach(processMonitor(_, syncState))
    }
  }

  def processMonitor(monitorConfig: MonitorConfig, syncState: List[SyncState]): Unit = {
    if (monitorConfig.restart != null) {
      if (monitorConfig.restart.heartbeatLostOver != -1) {
        val heartbeatOvers = componentManager.components.values.filter(_.heartbeatLost() > monitorConfig.restart.heartbeatLostOver)
        if (heartbeatOvers.size > 0) {
          logger.warn(s"Components heartbeat lost over ${monitorConfig.restart.heartbeatLostOver}, restarting ...")
          logger.warn(s"Component status detail ${jackson.writeValueAsString(heartbeatOvers)}")
          val redNames = heartbeatOvers.map(_.getName).toList
          ctx.restart(s"Component ${redNames.mkString(",")} heartbeat lost over ${monitorConfig.restart.heartbeatLostOver}")
        }
      }
      if (monitorConfig.restart.syncBlockedOver != -1 && ctx.syncStatus.blocked > monitorConfig.restart.syncBlockedOver) {
        logger.warn(s"Sync blocked count ${ctx.syncStatus.blocked} over ${monitorConfig.restart.syncBlockedOver}, restarting ...")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        ctx.restart(s"Sync blocked count ${ctx.syncStatus.blocked} over ${monitorConfig.restart.syncBlockedOver}")
      }
      if (monitorConfig.restart.syncErrorOver != -1 && ctx.syncStatus.blocked > monitorConfig.restart.syncErrorOver) {
        logger.warn(s"Sync error count ${ctx.syncStatus.error} over ${monitorConfig.restart.syncErrorOver}, restarting ...")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        ctx.restart(s"Sync error count ${ctx.syncStatus.error} over ${monitorConfig.restart.syncErrorOver}")
      }
      if (monitorConfig.restart.syncPendingOver != -1 && ctx.syncStatus.blocked > monitorConfig.restart.syncPendingOver) {
        logger.warn(s"Sync pending count ${ctx.syncStatus.pending} over ${monitorConfig.restart.syncPendingOver}, restarting ...")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        ctx.restart(s"Sync pending count ${ctx.syncStatus.pending} over ${monitorConfig.restart.syncPendingOver}")
      }
    }
    if (monitorConfig.alarm != null && monitorConfig.alarmType != null) {
      if (monitorConfig.alarm.heartbeatLostOver != -1) {
        val heartbeatOvers = componentManager.components.values.filter(_.heartbeatLost() > monitorConfig.alarm.heartbeatLostOver)
        if (heartbeatOvers.size > 0) {
          logger.warn(s"Components heartbeat lost over ${monitorConfig.alarm.heartbeatLostOver}, send alarm ...")
          logger.warn(s"Component status detail ${jackson.writeValueAsString(heartbeatOvers)}")
          val redNames = heartbeatOvers.map(_.getName).toList
          ctx.restart(s"Component ${redNames.mkString(",")} heartbeat lost over ${monitorConfig.restart.heartbeatLostOver}")
        }
      }
      if (monitorConfig.alarm.syncBlockedOver != -1 && ctx.syncStatus.blocked > monitorConfig.alarm.syncBlockedOver) {
        logger.warn(s"Sync blocked count ${ctx.syncStatus.blocked} over ${monitorConfig.alarm.syncBlockedOver}, send alarm ...")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        ctx.restart(s"Sync blocked count ${ctx.syncStatus.blocked} over ${monitorConfig.alarm.syncBlockedOver}")
      }
      if (monitorConfig.alarm.syncErrorOver != -1 && ctx.syncStatus.blocked > monitorConfig.alarm.syncErrorOver) {
        logger.warn(s"Sync error count ${ctx.syncStatus.error} over ${monitorConfig.alarm.syncErrorOver}, send alarm ...")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        ctx.restart(s"Sync error count ${ctx.syncStatus.error} over ${monitorConfig.alarm.syncErrorOver}")
      }
      if (monitorConfig.alarm.syncPendingOver != -1 && ctx.syncStatus.blocked > monitorConfig.alarm.syncPendingOver) {
        logger.warn(s"Sync pending count ${ctx.syncStatus.pending} over ${monitorConfig.alarm.syncPendingOver}, send alarm ...")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        ctx.restart(s"Sync pending count ${ctx.syncStatus.pending} over ${monitorConfig.alarm.syncPendingOver}")
      }
    }
  }

  def sendAlarm(components: Iterable[HeartbeatComponent], syncStatus: SyncState, reason: String): Unit = {

  }

  def destroy() = {
    timer.cancel()
  }

}
