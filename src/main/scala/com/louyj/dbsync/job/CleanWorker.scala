package com.louyj.dbsync.job

import com.louyj.dbsync.DatasourcePools
import com.louyj.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.dbsync.dbopt.DbOperationRegister
import com.louyj.dbsync.sync.IHeartableComponent
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class CleanWorker(dsPools: DatasourcePools,
                  sysConfig: SysConfig, dbConfigs: List[DatabaseConfig], interval: Long)
  extends IHeartableComponent {

  val logger = LoggerFactory.getLogger(getClass)
  setName("cronjob")
  start()

  override def run(): Unit = {
    logger.info(s"Start clean worker, scheduled at fixed rate of ${sysConfig.cleanInterval}ms")
    while (!this.isInterrupted) {
      TimeUnit.MILLISECONDS.sleep(interval)
      heartbeat()
      dbConfigs.foreach(cleanFun)
    }
    logger.info(s"Stop clean worker")
  }

  def cleanFun = (dbConfig: DatabaseConfig) => {
    try {
      logger.info(s"Start clean system tables for ${dbConfig.name}")
      val jdbcTemplate = dsPools.jdbcTemplate(dbConfig.name)
      val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
      val count = dbOpt.cleanSysTable(jdbcTemplate, dbConfig, sysConfig.dataKeepHours)
      logger.info(s"Finish clean system tables for ${dbConfig.name}, cleaned $count datas")
    } catch {
      case e: Exception => logger.warn("Clean task failed.", e)
    }
  }

  override def heartbeatInterval(): Long = interval
}
