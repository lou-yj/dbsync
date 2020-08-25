package com.louyj.tools.dbsync

import java.io.FileInputStream

import com.louyj.tools.dbsync.config.ConfigParser
import com.louyj.tools.dbsync.dbopt.DbOperationRegister
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.tools.dbsync.init.{DatabaseInitializer, TriggerInitializer}
import com.louyj.tools.dbsync.job.CleanWorker
import com.louyj.tools.dbsync.sync.{DataPoller, DataSyncer, QueueManager}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 15:11<br/>
 *
 * @author Louyj<br/>
 */

object DbSyncLanucher {

  val logger = LoggerFactory.getLogger(DbSyncLanucher.getClass)

  def main(args: Array[String]): Unit = {
    val stream = if (args.length > 0) new FileInputStream(args(0))
    else ClassLoader.getSystemResource("app.yaml").openStream()
    val configParser = new ConfigParser(stream)
    val dsPools = new DatasourcePools(configParser.databaseConfig)
    val sysConfig = configParser.sysConfig
    val syncConfigs = configParser.syncConfigMap
    val dbConfigs = configParser.databaseConfig
    val dbconfigsMap = configParser.databaseConfigMap

    new DatabaseInitializer(dsPools, dbConfigs)
    new CleanWorker(dsPools, sysConfig, dbConfigs)
    val queueManager = new QueueManager(sysConfig.partition)
    new DataSyncer(dbconfigsMap, queueManager, dsPools)

    val threads = new ListBuffer[Thread]
    dbConfigs.foreach(dbConfig => {
      val jdbc = dsPools.jdbcTemplate(dbConfig.name)
      new TriggerInitializer(dbConfig, dsPools, configParser.syncConfig, dbConfig.sysSchema)
      threads += new DataPoller(sysConfig, dbConfig, jdbc, queueManager, syncConfigs)
    })
    logger.info("Application lanuched")
    threads.foreach(_.join())

  }


}
