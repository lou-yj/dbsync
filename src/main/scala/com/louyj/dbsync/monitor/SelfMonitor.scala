package com.louyj.dbsync.monitor

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
  new Endpoints(ctx.app, componentManager, ctx)
  val timer = new Timer(true)
  timer.schedule(this, TimeUnit.MINUTES.toMillis(1),
    TimeUnit.MINUTES.toMillis(1))

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
  }

  def destroy() = {
    timer.cancel()
  }
}
