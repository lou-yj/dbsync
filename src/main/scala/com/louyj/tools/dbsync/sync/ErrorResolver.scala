package com.louyj.tools.dbsync.sync

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SysConfig}
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/25 9:53<br/>
 *
 * @author Louyj<br/>
 */

class ErrorResolver(sysConfig: SysConfig, queueManager: QueueManager, dsPools: DatasourcePools,
                    dbConfigs: Map[String, DatabaseConfig]) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  setName("error-resolver")
  start()

  override def run(): Unit = {
    while (!isInterrupted) {
      val errorBatch = queueManager.takeError


    }
  }

  def loopRetry(errorBatch: ErrorBatch): Unit = {
    var retry = new AtomicInteger(sysConfig.maxRetry)
    val sourceDb = errorBatch.sourceDb
    val targetDb = errorBatch.targetDb
    logger.warn(s"Receive ${errorBatch.ids.size} error data, table ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb], ids ${errorBatch.ids.mkString(",")} ")
    val srcJdbc = dsPools.jdbcTemplate(sourceDb)
    val tarJdbc = dsPools.jdbcTemplate(targetDb)
    (errorBatch.args zip errorBatch.ids zip errorBatch.hashs).foreach(triple => {
      val tuple = triple._1
      val args = tuple._1
      val id = tuple._2
      val hash = triple._2
      logger.info(s"Retry data ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id, will retry ${sysConfig.maxRetry} times interval ${sysConfig.retryInterval}ms")
      while (retry.decrementAndGet() > 0) {
        try {
          tarJdbc.update(errorBatch.sql, args: _*)
          logger.info(s"Successfully retry for ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id")
          queueManager.resolvedError(hash, id)
          retry.set(0)
        } catch {
          case e: InterruptedException => throw e
          case e: Exception => TimeUnit.MILLISECONDS.sleep(sysConfig.retryInterval)
        }
      }
    })

  }

  def retry() = {

  }

}
