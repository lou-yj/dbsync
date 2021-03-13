package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.dbsync.sync.ComponentManager
import com.louyj.dbsync.sync.ComponentStatus.{GREEN, RED, YELLOW}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import java.util.{ServiceLoader, Timer, TimerTask}
import scala.collection.mutable

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

  import scala.collection.JavaConverters._

  val alarmSenders: Map[String, ActionHandler] = ServiceLoader.load(classOf[ActionHandler], Thread.currentThread.getContextClassLoader)
    .iterator().asScala.map(v => (v.name(), v)).toMap

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
    if (monitorConfig.matches != null && monitorConfig.action != null) {
      if (monitorConfig.matches.heartbeatLostOver != -1) {
        val heartbeatOvers = componentManager.components.filter(_._2.heartbeatLost() > monitorConfig.matches.heartbeatLostOver)
        if (heartbeatOvers.size > 0) {
          val heartbeatOversDetails = componentManager.format(heartbeatOvers)
          logger.warn(s"Components heartbeat lost over ${monitorConfig.matches.heartbeatLostOver}")
          logger.warn(s"Component status detail $heartbeatOversDetails")
          val redNames = heartbeatOvers.map(_._2.getName).toList
          sendAlarm(monitorConfig.action, monitorConfig.params, heartbeatOversDetails, null,
            s"Component ${redNames.mkString(",")} heartbeat lost over ${monitorConfig.matches.heartbeatLostOver}")
        }
      }
      if (monitorConfig.matches.syncBlockedOver != -1 && ctx.syncStatus.blocked > monitorConfig.matches.syncBlockedOver) {
        logger.warn(s"Sync blocked count ${ctx.syncStatus.blocked} over ${monitorConfig.matches.syncBlockedOver}")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        sendAlarm(monitorConfig.action, monitorConfig.params, null, ctx.syncStatus,
          s"Sync blocked count ${ctx.syncStatus.blocked} over ${monitorConfig.matches.syncBlockedOver}")
      }
      if (monitorConfig.matches.syncErrorOver != -1 && ctx.syncStatus.blocked > monitorConfig.matches.syncErrorOver) {
        logger.warn(s"Sync error count ${ctx.syncStatus.error} over ${monitorConfig.matches.syncErrorOver}")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        sendAlarm(monitorConfig.action, monitorConfig.params, null, ctx.syncStatus,
          s"Sync error count ${ctx.syncStatus.error} over ${monitorConfig.matches.syncErrorOver}")
      }
      if (monitorConfig.matches.syncPendingOver != -1 && ctx.syncStatus.blocked > monitorConfig.matches.syncPendingOver) {
        logger.warn(s"Sync pending count ${ctx.syncStatus.pending} over ${monitorConfig.matches.syncPendingOver}")
        logger.warn(s"Sync status detail ${jackson.writeValueAsString(syncState)}")
        sendAlarm(monitorConfig.action, monitorConfig.params, null, ctx.syncStatus,
          s"Sync pending count ${ctx.syncStatus.pending} over ${monitorConfig.matches.syncPendingOver}")
      }
    }
  }

  def sendAlarm(alarmType: String, alarmArgs: Map[String, Object], components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String): Unit = {
    alarmSenders.get(alarmType) match {
      case None => logger.warn(s"No such action implement: $alarmType")
      case Some(alarmSender) => alarmSender.doAction(ctx, components, syncStatus, reason, alarmArgs)
    }
  }

  def destroy() = {
    timer.cancel()
  }

}
