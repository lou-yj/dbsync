package com.louyj.dbsync.component.job

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.component.{ComponentConfigAware, ScheduleComponent}
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.dbopt.DbOperationRegister
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class CleanWorker(ctx: SystemContext)
  extends ScheduleComponent with ComponentConfigAware {

  val logger = LoggerFactory.getLogger(getClass)

  override def name = "cleanWorker"

  override def open(ctx: SystemContext): Unit = super.open(ctx)

  override def run(): Unit = {
    ctx.appConfig.db.foreach(cleanFun)
  }

  def cleanFun = (dbConfig: DatabaseConfig) => {
    try {
      logger.info(s"Start clean system tables for ${dbConfig.name}")
      val jdbcTemplate = ctx.dsPools.jdbcTemplate(dbConfig.name)
      val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
      val count = dbOpt.cleanSysTable(jdbcTemplate, dbConfig, ctx.sysConfig.dataKeepHours)
      logger.info(s"Finish clean system tables for ${dbConfig.name}, cleaned $count datas")
    } catch {
      case e: Exception => logger.warn("Clean task failed.", e)
    }
  }

  override def rate(): Long = ???

  override def delay(): Long = ???

}
