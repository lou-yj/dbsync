package com.louyj.dbsync.job

import com.louyj.dbsync.App.logger
import com.louyj.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.dbsync.sync.HeartbeatComponent
import com.louyj.dbsync.{DatasourcePools, SystemContext}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class SyncTrigger(ctx: SystemContext)
  extends HeartbeatComponent with TriggerSync {

  val logger = LoggerFactory.getLogger(getClass)
  setName("syncTrigger")
  start()
  var lastExecute = System.currentTimeMillis()

  override def run(): Unit = {
    logger.info(s"Start sync trigger worker, scheduled at fixed rate of ${ctx.sysConfig.syncTriggerInterval}ms")
    while (ctx.running) {
      TimeUnit.MILLISECONDS.sleep(5000)
      heartbeat()
      if (System.currentTimeMillis() - lastExecute > ctx.sysConfig.syncTriggerInterval) {
        for (dbConfig <- ctx.dbConfigs;
             syncConfig <- ctx.syncConfigs if syncConfig.sourceDb == dbConfig.name) {
          try {
            syncConfig.targetDb.split(",").foreach(targetDb => {
              val tarDbConfig = ctx.dbConfigsMap(targetDb)
              syncTrigger(dbConfig, tarDbConfig, ctx.dsPools, syncConfig)
            })
          } catch {
            case e: Throwable => logger.warn("Sync trigger job failed.", e);
          }
        }
        lastExecute = System.currentTimeMillis()
      }
    }
    logger.info(s"Stop sync trigger worker")
  }

  override def heartbeatInterval(): Long = 5
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


  def cleanTrigger(dsPools: DatasourcePools, dbConfig: DatabaseConfig, syncConfigs: List[SyncConfig]) = {
    logger.info(s"Start clean trigger for ${dbConfig.name}")
    val syncSet = syncConfigs.filter(_.sourceDb == dbConfig.name).map(sync => s"${sync.sourceSchema}:${sync.sourceTable}").toSet
    val dbOpt = dbOpts(dbConfig.`type`)
    val jdbc = dsPools.jdbcTemplate(dbConfig.name)
    dbOpt.listTriggers(dbConfig, jdbc).foreach(st => {
      val key = s"${st.schema}:${st.table}"
      if (!syncSet.contains(key)) {
        logger.info(s"Config for table ${st.schema}.${st.table}[${dbConfig.name}] not exists, delete trigger ${st.trigger}")
        dbOpt.deleteTrigger(dbConfig, jdbc, st.schema, st.table, st.trigger, st.function)
        logger.info(s"Trigger ${st.trigger} for table ${st.schema}.${st.table}[${dbConfig.name}] deleted")
      }
    })
    logger.info(s"Finished clean trigger for ${dbConfig.name}")
  }

  def syncTrigger(srcDbConfig: DatabaseConfig, tarDbConfig: DatabaseConfig,
                  dsPools: DatasourcePools, syncConfig: SyncConfig) = {
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
    } else {
      logger.info(s"Create index for $tarDbName disabled.")
    }
  }

}
