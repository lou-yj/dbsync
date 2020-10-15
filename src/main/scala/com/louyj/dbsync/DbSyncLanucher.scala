package com.louyj.dbsync

import java.io.FileInputStream

import com.louyj.dbsync.config.ConfigParser
import com.louyj.dbsync.endpoint.Endpoints
import com.louyj.dbsync.init.{DatabaseInitializer, TriggerInitializer}
import com.louyj.dbsync.job.{BootstrapTriggerSync, CleanWorker, JobScheduler}
import com.louyj.dbsync.sync._
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
    val syncConfigsMap = configParser.syncConfigMap
    val syncConfigs = configParser.syncConfig
    val dbConfigs = configParser.databaseConfig
    val dbconfigsMap = configParser.databaseConfigMap

    new DatabaseInitializer(dsPools, dbConfigs)
    new CleanWorker(dsPools, sysConfig, dbConfigs)

    val stateManager = new StateManger(sysConfig, dbConfigs, dsPools)
    val queueManager = new QueueManager(sysConfig.partition, stateManager, sysConfig)
    new DataSyncer(dbconfigsMap, queueManager, dsPools)
    new ErrorResolver(sysConfig, queueManager, dsPools, dbconfigsMap)
    new BlockedHandler(sysConfig, queueManager, dsPools, dbconfigsMap)

    val threads = new ListBuffer[Thread]
    dbConfigs.foreach(dbConfig => {
      val jdbc = dsPools.jdbcTemplate(dbConfig.name)
      new TriggerInitializer(dbConfig, dbconfigsMap, dsPools, syncConfigs) with BootstrapTriggerSync
      threads += new DataPoller(sysConfig, dbConfig, jdbc, queueManager, syncConfigsMap)
    })
    new JobScheduler(dsPools, sysConfig, dbConfigs, dbconfigsMap, syncConfigs)
    new Endpoints(sysConfig, dbConfigs, dsPools)
    logger.info("Application lanuched")
    threads.foreach(_.join())

  }


}
