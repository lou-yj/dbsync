package com.louyj.tools.dbsync

import java.io.FileInputStream

import com.louyj.tools.dbsync.config.{ConfigParser, DbContext}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister
import com.louyj.tools.dbsync.sync.{DataPoller, DataSyncer, QueueManager}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 15:11<br/>
 *
 * @author Louyj<br/>
 */

object DbsyncLanucher {

  val logger = LoggerFactory.getLogger(DbsyncLanucher.getClass)

  def main(args: Array[String]): Unit = {
    val stream = if (args.length > 0) new FileInputStream(args(0))
    else ClassLoader.getSystemResource("app.yaml").openStream()
    val configParser = new ConfigParser(stream)
    val datasourcePools = new DatasourcePools(configParser.databaseConfig)
    val sysConfig = configParser.sysConfig
    val syncConfigs = configParser.syncConfigMap
    val dbOpts = DbOperationRegister.dbOpts

    val threads = new ListBuffer[Thread]
    configParser.databaseConfig.foreach(dbConfig => {
      logger.info("Setup sync workers for database {}", dbConfig.name)
      val queueManager = new QueueManager(sysConfig.partition)
      val jdbcTemplate = datasourcePools.jdbcTemplate(dbConfig.name)
      val dbContext = DbContext(queueManager, dbConfig, syncConfigs, jdbcTemplate, datasourcePools, dbOpts, sysConfig)
      val pollThread = new DataPoller(dbContext)
      new DataSyncer(dbContext)
      threads += pollThread
    })
    logger.info("Application lanuched")
    threads.foreach(_.join())

  }


}
