package com.louyj.dbsync

import com.louyj.dbsync.config.ConfigParser
import com.louyj.dbsync.init.{DatabaseInitializer, TriggerInitializer}
import com.louyj.dbsync.job.{BootstrapTriggerSync, CleanWorker, SyncTrigger}
import com.louyj.dbsync.monitor.SelfMonitor
import com.louyj.dbsync.sync._
import org.slf4j.LoggerFactory

import java.io.FileInputStream
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
    val stateManager = new StateManger(sysConfig, dbConfigs, dsPools)
    val queueManager = new QueueManager(sysConfig.partition, stateManager, sysConfig)
    val dataSyncer = new DataSyncer(dbconfigsMap, queueManager, dsPools)
    val errorResolver = new ErrorResolver(sysConfig, queueManager, dsPools, dbconfigsMap)
    val blockedHandler = new BlockedHandler(sysConfig, queueManager, dsPools, dbconfigsMap)

    val dataPollers = new ListBuffer[HeartbeatComponent]
    dbConfigs.foreach(dbConfig => {
      val jdbc = dsPools.jdbcTemplate(dbConfig.name)
      new TriggerInitializer(dbConfig, dbconfigsMap, dsPools, syncConfigs) with BootstrapTriggerSync
      val dataPoller = new DataPoller(sysConfig, dbConfig, jdbc, queueManager, syncConfigsMap)
      dataPollers += dataPoller
    })

    val cleanWorker = new CleanWorker(dsPools, sysConfig, dbConfigs, sysConfig.cleanInterval)
    val syncTrigger = new SyncTrigger(dsPools, dbConfigs, dbconfigsMap, syncConfigs, sysConfig.syncTriggerInterval)

    val componentManager = new ComponentManager
    componentManager.addComponents(dataPollers.toList)
    componentManager.addComponents(dataSyncer.sendWorkers)
    componentManager.addComponents(errorResolver, blockedHandler, cleanWorker, syncTrigger)
    new SelfMonitor(sysConfig, dbConfigs, dsPools, componentManager)

    logger.info("Application lanuched")
    dataPollers.foreach(_.join())

  }


}
