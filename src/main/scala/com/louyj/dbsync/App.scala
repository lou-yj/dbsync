package com.louyj.dbsync

import com.louyj.dbsync.component.state.{QueueManager, StateManger}
import com.louyj.dbsync.component.{ComponentManager, HeartbeatComponent}
import com.louyj.dbsync.config.ConfigParser
import com.louyj.dbsync.init.{DatabaseInitializer, TriggerInitializer}
import com.louyj.dbsync.job.{BootstrapTriggerSync, CleanWorker, SyncTrigger}
import com.louyj.dbsync.monitor.SelfMonitor
import com.louyj.dbsync.sync.{BlockedHandler, DataPoller, DataSyncer, ErrorResolver}
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import scala.collection.mutable.ListBuffer

/**
 * @Author: Louyj
 * @Date: Created at 2021/5/13
 *
 */
object App {
  val logger = LoggerFactory.getLogger(App.getClass)

  def bootstrap(args: Array[String], restartReason: String = "N/A"): Unit = {
    val stream = if (args.length > 0) new FileInputStream(args(0)) else ClassLoader.getSystemResource("app.yaml").openStream()
    val configParser = new ConfigParser(stream)
    val ctx = new SystemContext(configParser, new DatasourcePools(configParser.databaseConfig), restartReason)
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
    //cronjob
    val cleanWorker = new CleanWorker(ctx)
    val syncTrigger = new SyncTrigger(ctx)
    //component monitor
    val componentManager = new ComponentManager
    componentManager.addComponents(dataPollers.toList)
    componentManager.addComponents(dataSyncer.sendWorkers)
    componentManager.addComponents(errorResolver, blockedHandler, cleanWorker, syncTrigger)
    val selfMonitor = new SelfMonitor(componentManager, ctx)

    logger.info("Application launched")
    dataPollers.foreach(_.join)
    dataSyncer.sendWorkers.foreach(_.join)
    cleanWorker.join
    syncTrigger.join
    selfMonitor.destroy()
    ctx.destroy()
    logger.info("Application exited")

  }

}