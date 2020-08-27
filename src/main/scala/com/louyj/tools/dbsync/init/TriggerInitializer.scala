package com.louyj.tools.dbsync.init

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.tools.dbsync.job.TriggerSync
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 16:12<br/>
 *
 * @author Louyj<br/>
 */


class TriggerInitializer(srcDbConfig: DatabaseConfig, dbconfigs: Map[String, DatabaseConfig],
                         dsPools: DatasourcePools,
                         syncConfigs: List[SyncConfig]) extends TriggerSync {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info(s"Start check trigger status for ${srcDbConfig.name}")
  syncConfigs.filter(_.sourceDb == srcDbConfig.name).foreach(buildTrigger)
  logger.info(s"Finish check trigger status for ${srcDbConfig.name}")


  def buildTrigger(syncConfig: SyncConfig) = {
    val dbName = syncConfig.sourceDb
    val dbOpt = dbOpts(srcDbConfig.`type`)
    val jdbc = dsPools.jdbcTemplate(dbName)
    val exists = dbOpt.tableExists(jdbc, syncConfig.sourceSchema, syncConfig.sourceTable)
    if (exists) {
      val tarDbConfig = dbconfigs(syncConfig.targetDb)
      syncTrigger(srcDbConfig, tarDbConfig, dsPools, syncConfig, true)
    } else {
      logger.error(s"Config check failed, table ${syncConfig.sourceSchema}.${syncConfig.sourceTable}[${dbName}] not exists")
      logger.error("Config check failed, system exit")
      System.exit(-1)
    }
  }

}
