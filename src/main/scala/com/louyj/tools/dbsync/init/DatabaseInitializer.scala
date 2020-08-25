package com.louyj.tools.dbsync.init

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.DatabaseConfig
import com.louyj.tools.dbsync.dbopt.DbOperationRegister
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 17:38<br/>
 *
 * @author Louyj<br/>
 */

class DatabaseInitializer(dsPools: DatasourcePools, dbConfigs: List[DatabaseConfig]) {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Start check system table status")
  dbConfigs.foreach(initDb)
  logger.info("Finish check system table status")

  def initDb(dbConfig: DatabaseConfig) = {
    val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
    val jdbcTemplate = dsPools.jdbcTemplate(dbConfig.name)
    dbOpt.buildSysTable(dbConfig.name, jdbcTemplate, dbConfig.sysSchema)
  }

}
