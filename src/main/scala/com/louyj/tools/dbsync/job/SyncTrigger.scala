package com.louyj.tools.dbsync.job

import java.util.TimerTask

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.DbSyncLanucher.logger
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class SyncTrigger(dsPools: DatasourcePools, dbConfigs: List[DatabaseConfig],
                  dbconfigsMap: Map[String, DatabaseConfig],
                  syncConfigs: List[SyncConfig])
  extends TimerTask with TriggerSync {

  val logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    for (dbconfig <- dbConfigs;
         syncConfig <- syncConfigs if syncConfig.sourceDb == dbconfig.name) {
      val tarDbConfig = dbconfigsMap(syncConfig.targetDb)
      syncTrigger(dbconfig, tarDbConfig, dsPools, syncConfig, false)
    }
  }

}

trait TriggerSync {

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
