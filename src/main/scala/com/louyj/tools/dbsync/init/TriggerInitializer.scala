package com.louyj.tools.dbsync.init

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DbContext, SyncConfig}
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 16:12<br/>
 *
 * @author Louyj<br/>
 */

class TriggerInitializer(dbContext: DbContext, dsPools: DatasourcePools, syncConfigs: List[SyncConfig], sysSchema: String) {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Start check trigger status")
  syncConfigs.foreach(buildTrigger)
  logger.info("Finish check trigger status")

  def buildTrigger(syncConfig: SyncConfig) = {
    val dbName = syncConfig.sourceDb
    val dbOpt = dbContext.dbOpts(dbName)
    val jdbcTemplate = dsPools.jdbcTemplate(dbName)
    dbOpt.buildInsertTrigger(dbName, sysSchema, jdbcTemplate, syncConfig)
    dbOpt.buildUpdateTrigger(dbName, sysSchema, jdbcTemplate, syncConfig)
    dbOpt.buildDeleteTrigger(dbName, sysSchema, jdbcTemplate, syncConfig)
  }

}
