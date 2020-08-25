package com.louyj.tools.dbsync.init

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 16:12<br/>
 *
 * @author Louyj<br/>
 */

class TriggerInitializer(dbConfig: DatabaseConfig,
                         dsPools: DatasourcePools,
                         syncConfigs: List[SyncConfig], sysSchema: String) {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info(s"Start check trigger status for ${dbConfig.name}")
  syncConfigs.filter(_.sourceDb == dbConfig.name).foreach(buildTrigger)
  logger.info(s"Finish check trigger status for ${dbConfig.name}")

  def buildTrigger(syncConfig: SyncConfig) = {
    val dbName = syncConfig.sourceDb
    val dbOpt = dbOpts(dbConfig.`type`)
    val jdbc = dsPools.jdbcTemplate(dbName)
    dbOpt.buildInsertTrigger(dbName, sysSchema, jdbc, syncConfig)
    dbOpt.buildUpdateTrigger(dbName, sysSchema, jdbc, syncConfig)
    dbOpt.buildDeleteTrigger(dbName, sysSchema, jdbc, syncConfig)
  }

}
