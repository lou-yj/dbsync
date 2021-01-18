package com.louyj.dbsync.job

import java.util.TimerTask

import com.louyj.dbsync.DatasourcePools
import com.louyj.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.dbsync.dbopt.DbOperationRegister
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class CleanWorker(dsPools: DatasourcePools,
                  sysConfig: SysConfig, dbConfigs: List[DatabaseConfig])
  extends TimerTask {

  val logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    def cleanFun = (dbConfig: DatabaseConfig) => {
      try {
        logger.info(s"Start clean system tables for ${dbConfig.name}")
        val jdbcTemplate = dsPools.jdbcTemplate(dbConfig.name)
        val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
        val count = dbOpt.cleanSysTable(jdbcTemplate, dbConfig, sysConfig.dataKeepHours)
        logger.info(s"Finish clean system tables for ${dbConfig.name}, cleaned $count datas")
      } catch {
        case e => logger.warn("Clean task failed.", e)
      }
    }

    dbConfigs.foreach(cleanFun)
  }

}
