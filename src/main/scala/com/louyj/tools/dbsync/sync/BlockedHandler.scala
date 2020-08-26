package com.louyj.tools.dbsync.sync

import java.util.concurrent.TimeUnit

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/25 11:13<br/>
 *
 * @author Louyj<br/>
 */

class BlockedHandler(sysConfig: SysConfig, queueManager: QueueManager,
                     dsPools: DatasourcePools, dbConfigs: Map[String, DatabaseConfig]) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  setName("blocked-handler")
  start()

  override def run(): Unit = {
    while (!isInterrupted) {
      try {
        val blockedData = queueManager.takeBlocked()
        val data = blockedData.data
        val sourceDb = data.sourceDb
        val dbConfig = dbConfigs(sourceDb)
        val jdbc = dsPools.jdbcTemplate(sourceDb)
        val hash = data.items.head.hash
        val id = data.items.head.id
        val partition = math.abs(hash % sysConfig.partition).intValue()
        val dbOpt = dbOpts(dbConfig.`type`)
        logger.warn(s"Data ${id}[$sourceDb] blocked by ${blockedData.blockedBy.mkString(",")}")
        dbOpt.markBlocked(dbConfig.sysSchema, jdbc, blockedData, partition)
      } catch {
        case e: InterruptedException => throw e
        case e: Exception => {
          logger.error("Blocked handler failed", e)
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }
  }

}
