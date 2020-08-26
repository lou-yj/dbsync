package com.louyj.tools.dbsync.init

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.DbSyncLanucher.logger
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.tools.dbsync.init.funcs.syncTrigger
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 16:12<br/>
 *
 * @author Louyj<br/>
 */

package object funcs {

  def syncTrigger = (srcDbConfig: DatabaseConfig, tarDbConfig: DatabaseConfig, dsPools: DatasourcePools, syncConfig: SyncConfig, init: Boolean) => {
    val srcDbName = syncConfig.sourceDb
    val srcDbOpt = dbOpts(srcDbConfig.`type`)
    val srcJdbc = dsPools.jdbcTemplate(srcDbName)
    val srcSysSchema = srcDbConfig.sysSchema
    val exists = srcDbOpt.tableExists(srcJdbc, syncConfig.sourceSchema, syncConfig.sourceTable)
    if (exists) {
      srcDbOpt.buildInsertTrigger(srcDbName, srcSysSchema, srcJdbc, syncConfig)
      srcDbOpt.buildUpdateTrigger(srcDbName, srcSysSchema, srcJdbc, syncConfig)
      srcDbOpt.buildDeleteTrigger(srcDbName, srcSysSchema, srcJdbc, syncConfig)

      if (tarDbConfig.createIndex) {
        val tarDbName = syncConfig.targetDb
        val tarDbOpt = dbOpts(tarDbConfig.`type`)
        val tarJdbc = dsPools.jdbcTemplate(tarDbName)
        val tarTableExists = tarDbOpt.tableExists(tarJdbc, syncConfig.targetSchema, syncConfig.targetTable)
        if (tarTableExists) {
          val indexColumns = syncConfig.sourceKeys.split(",").sorted.mkString(",")
          val indexExists = tarDbOpt.uniqueIndexExists(tarJdbc, syncConfig.targetSchema, syncConfig.targetTable, indexColumns)
          if (!indexExists) {
            logger.info(s"Unique index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] not exists, rebuild it")
            try {
              tarDbOpt.createUniqueIndex(tarJdbc, syncConfig.targetSchema, syncConfig.targetTable, indexColumns)
              logger.info(s"Unique index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] created")
            } catch {
              case e: Exception => logger.error(s"Create unique index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] failed, reason ${e.getMessage}")
                if (init) {
                  logger.error("Setup failed, system exit")
                  System.exit(-1)
                }
            }
          } else {
            logger.debug(s"Index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] already exists and matched")
          }
        } else {
          logger.warn(s"Target table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] not exists")
        }
      }
    } else {
      logger.error(s"Trigger check failed, table ${syncConfig.sourceSchema}.${syncConfig.sourceTable}[$srcDbName] not exists")
      if (init) {
        logger.error("Config check failed, system exit")
        System.exit(-1)
      }
    }
  }

}

class TriggerInitializer(srcDbConfig: DatabaseConfig, dbconfigs: Map[String, DatabaseConfig],
                         dsPools: DatasourcePools,
                         syncConfigs: List[SyncConfig]) {

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
