package com.louyj.dbsync.job

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.dbopt.DbOperationRegister
import com.louyj.dbsync.sync.HeartbeatComponent
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class CleanWorker(ctx: SystemContext)
  extends HeartbeatComponent {

  val logger = LoggerFactory.getLogger(getClass)
  setName("cleanWorker")
  start()

  var lastExecute = System.currentTimeMillis()

  override def run(): Unit = {
    logger.info(s"Start clean worker, scheduled at fixed rate of ${ctx.sysConfig.cleanInterval}ms")
    while (ctx.running) {
      TimeUnit.MILLISECONDS.sleep(5000)
      heartbeat()
      if (System.currentTimeMillis() - lastExecute > ctx.sysConfig.cleanInterval) {
        ctx.dbConfigs.foreach(cleanFun)
        lastExecute = System.currentTimeMillis()
      }
    }
    logger.info(s"Stop clean worker")
  }

  def cleanFun = (dbConfig: DatabaseConfig) => {
    try {
      logger.info(s"Start clean system tables for ${dbConfig.name}")
      val jdbcTemplate = ctx.dsPools.sysJdbcTemplate(dbConfig.name)
      val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
      val count = dbOpt.cleanSysTable(jdbcTemplate, dbConfig, ctx.sysConfig.dataKeepHours)
      logger.info(s"Finish clean system tables for ${dbConfig.name}, cleaned $count datas")
    } catch {
      case e: Exception => logger.warn("Clean task failed.", e)
    }
  }

  override def heartbeatInterval(): Long = 5
}
