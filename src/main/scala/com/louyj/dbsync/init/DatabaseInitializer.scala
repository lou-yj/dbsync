package com.louyj.dbsync.init

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.dbopt.DbOperationRegister
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 17:38<br/>
 *
 * @author Louyj<br/>
 */

class DatabaseInitializer(ctx: SystemContext) {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Start check system table status")
  ctx.dbConfigs.foreach(initDb)
  logger.info("Finish check system table status")

  def initDb(dbConfig: DatabaseConfig) = {
    val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
    val jdbcTemplate = ctx.dsPools.sysJdbcTemplate(dbConfig.name)
    dbOpt.buildSysTable(dbConfig, jdbcTemplate)
  }

}
