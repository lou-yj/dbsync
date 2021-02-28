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
    val ctx = new SystemContext(configParser, new DatasourcePools(configParser.databaseConfig))

    //    val sysConfig = configParser.sysConfig
    //    val syncConfigsMap = configParser.syncConfigMap
    //    val syncConfigs = configParser.syncConfig
    //    val dbConfigs = configParser.databaseConfig
    //    val dbconfigsMap = configParser.databaseConfigMap

    new DatabaseInitializer(ctx)
    val stateManager = new StateManger(ctx)
    val queueManager = new QueueManager(stateManager, ctx)
    val dataSyncer = new DataSyncer(queueManager, ctx)
    val errorResolver = new ErrorResolver(queueManager, ctx)
    val blockedHandler = new BlockedHandler(queueManager, ctx)

    val dataPollers = new ListBuffer[HeartbeatComponent]
    ctx.dbConfigs.foreach(dbConfig => {
      val jdbc = ctx.dsPools.jdbcTemplate(dbConfig.name)
      new TriggerInitializer(dbConfig, ctx) with BootstrapTriggerSync
      val dataPoller = new DataPoller(dbConfig, jdbc, queueManager, ctx)
      dataPollers += dataPoller
    })

    val cleanWorker = new CleanWorker(ctx)
    val syncTrigger = new SyncTrigger(ctx)

    val componentManager = new ComponentManager
    componentManager.addComponents(dataPollers.toList)
    componentManager.addComponents(dataSyncer.sendWorkers)
    componentManager.addComponents(errorResolver, blockedHandler, cleanWorker, syncTrigger)
    new SelfMonitor(componentManager, ctx)

    logger.info("Application lanuched")
    dataPollers.foreach(_.join())

  }


}
