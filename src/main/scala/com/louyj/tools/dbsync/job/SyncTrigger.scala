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
      syncConfig.targetDb.split(",").foreach(targetdb => {
        val tarDbConfig = dbconfigsMap(targetdb)
        syncTrigger(dbconfig, tarDbConfig, dsPools, syncConfig)
      })
    }
  }

}


trait BootstrapTriggerSync extends TriggerSync {

  override def onError(reason: String): Unit = {
    logger.error(s"$reason, system exit")
    System.exit(-1)
  }

}


trait TriggerSync {

  def onError(reason: String) = {

  }

  def syncTrigger(srcDbConfig: DatabaseConfig, tarDbConfig: DatabaseConfig, dsPools: DatasourcePools, syncConfig: SyncConfig) = {
    val srcDbName = syncConfig.sourceDb
    val srcDbOpt = dbOpts(srcDbConfig.`type`)
    val srcJdbc = dsPools.jdbcTemplate(srcDbName)
    val exists = srcDbOpt.tableExists(srcJdbc, syncConfig.sourceSchema, syncConfig.sourceTable)
    if (exists) {
      srcDbOpt.buildInsertTrigger(srcDbConfig, srcJdbc, syncConfig)
      srcDbOpt.buildUpdateTrigger(srcDbConfig, srcJdbc, syncConfig)
      srcDbOpt.buildDeleteTrigger(srcDbConfig, srcJdbc, syncConfig)
      checkIndex(tarDbConfig, syncConfig, dsPools)
    } else {
      logger.error(s"Trigger check failed, table ${syncConfig.sourceSchema}.${syncConfig.sourceTable}[$srcDbName] not exists")
      onError("Trigger check failed")
    }
  }

  private def checkIndex(tarDbConfig: DatabaseConfig, syncConfig: SyncConfig, dsPools: DatasourcePools) = {
    val tarDbName = tarDbConfig.name
    val tarDbOpt = dbOpts(tarDbConfig.`type`)
    val tarJdbc = dsPools.jdbcTemplate(tarDbName)
    val check: PartialFunction[Boolean, Unit] = {
      case true =>
        val indexColumns = syncConfig.sourceKeys.split(",").sorted.mkString(",")
        if (tarDbOpt.uniqueIndexExists(tarJdbc, syncConfig.targetSchema, syncConfig.targetTable, indexColumns)) {
          logger.debug(s"Index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] already exists and matched")
        } else {
          logger.info(s"Unique index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] not exists, rebuild it")
          try {
            tarDbOpt.createUniqueIndex(tarJdbc, syncConfig.targetSchema, syncConfig.targetTable, indexColumns)
            logger.info(s"Unique index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] created")
          } catch {
            case e: Exception => logger.error(s"Create unique index $indexColumns for table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] failed, reason ${e.getMessage}")
              onError("Index setup failed")
          }
        }
      case false =>
        logger.warn(s"Target table ${syncConfig.targetSchema}.${syncConfig.targetTable}[$tarDbName] not exists")
    }
    if (tarDbConfig.createIndex) {
      check.apply(tarDbOpt.tableExists(tarJdbc, syncConfig.targetSchema, syncConfig.targetTable))
    }
  }

}
