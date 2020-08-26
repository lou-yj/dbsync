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

  def syncTrigger = (dbConfig: DatabaseConfig, dsPools: DatasourcePools, syncConfig: SyncConfig) => {
    val dbName = syncConfig.sourceDb
    val dbOpt = dbOpts(dbConfig.`type`)
    val jdbc = dsPools.jdbcTemplate(dbName)
    val sysSchema = dbConfig.sysSchema
    dbOpt.buildInsertTrigger(dbName, sysSchema, jdbc, syncConfig)
    dbOpt.buildUpdateTrigger(dbName, sysSchema, jdbc, syncConfig)
    dbOpt.buildDeleteTrigger(dbName, sysSchema, jdbc, syncConfig)
    if (dbConfig.createIndex) {
      val indexColumns = syncConfig.sourceKeys.split(",").sorted.mkString(",")
      val indexExists = dbOpt.uniqueIndexExists(jdbc, syncConfig.sourceSchema, syncConfig.sourceTable, indexColumns)
      if (!indexExists) {
        logger.info(s"Unique index $indexColumns for table {}.{}[{}] not exists, rebuild it", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
        try {
          dbOpt.createUniqueIndex(jdbc, syncConfig.sourceSchema, syncConfig.sourceTable, indexColumns)
          logger.info(s"Unique index $indexColumns for table {}.{}[{}] created", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
        } catch {
          case e: Exception => logger.error(s"Create unique index $indexColumns for table ${syncConfig.sourceSchema}.${syncConfig.sourceTable}[$dbName] failed, reason ${e.getMessage}")
            logger.error("Setup failed, system exit")
            System.exit(-1)
        }
      } else {
        logger.info(s"Index $indexColumns for table {}.{}[{}] already exists and matched", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
      }
    }
  }

}

class TriggerInitializer(dbConfig: DatabaseConfig,
                         dsPools: DatasourcePools,
                         syncConfigs: List[SyncConfig]) {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info(s"Start check trigger status for ${dbConfig.name}")
  syncConfigs.filter(_.sourceDb == dbConfig.name).foreach(buildTrigger)
  logger.info(s"Finish check trigger status for ${dbConfig.name}")


  def buildTrigger(syncConfig: SyncConfig) = {
    val dbName = syncConfig.sourceDb
    val dbOpt = dbOpts(dbConfig.`type`)
    val jdbc = dsPools.jdbcTemplate(dbName)
    val exists = dbOpt.tableExists(jdbc, syncConfig.sourceSchema, syncConfig.sourceTable)
    if (exists) {
      syncTrigger(dbConfig, dsPools, syncConfig)
    } else {
      logger.error(s"Config check failed, table ${syncConfig.sourceSchema}.${syncConfig.sourceTable}[${dbName}] not exists")
      logger.error("Config check failed, system exit")
      System.exit(-1)
    }
  }

}
