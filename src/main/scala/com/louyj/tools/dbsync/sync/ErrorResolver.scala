package com.louyj.tools.dbsync.sync

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
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
    logger.info("Error resolver worker lanuched")
    while (!isInterrupted) {
      try {
        val errorBatch = queueManager.takeError
        loopRetry(errorBatch)
      } catch {
        case e: InterruptedException => throw e
        case e: Exception => {
          logger.info("Error resolve failed", e)
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }
  }

  def loopRetry(errorBatch: ErrorBatch): Unit = {
    val sourceDb = errorBatch.sourceDb
    val targetDb = errorBatch.targetDb
    logger.warn(s"Receive ${errorBatch.ids.size} error data, table ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb], ids ${errorBatch.ids.mkString(",")} ")
    val srcJdbc = dsPools.jdbcTemplate(sourceDb)
    val tarJdbc = dsPools.jdbcTemplate(targetDb)
    (errorBatch.args zip errorBatch.ids zip errorBatch.hashs).foreach(triple => {
      val retry = new AtomicInteger(sysConfig.maxRetry)
      val tuple = triple._1
      val args = tuple._1
      val id = tuple._2
      val hash = triple._2
      val partition = math.abs(hash % sysConfig.partition).intValue()
      val dbConfig = dbConfigs(sourceDb)
      val dbOpt = dbOpts(dbConfig.`type`);
      logger.info(s"Retry data ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id, will retry ${sysConfig.maxRetry} times interval ${sysConfig.retryInterval}ms")
      while (retry.decrementAndGet() > 0) {
        try {
          tarJdbc.update(errorBatch.sql, args: _*)
          val ackSql = dbOpt.batchAck(srcJdbc, dbConfig.sysSchema, List(id), "OK", s"success with ${sysConfig.maxRetry - retry.get()} retry")
          logger.info(s"Successfully retry for ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id")
          queueManager.resolvedError(partition, hash, id)
          retry.set(0)
        } catch {
          case e: InterruptedException => throw e
          case e: Exception => {
            val message = s"Retry failed ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id, reason ${e.getClass.getSimpleName}-${e.getMessage}"
            logger.warn(message)
            dbOpt.batchAck(srcJdbc, dbConfig.sysSchema, List(id), "ERR", message)
            TimeUnit.MILLISECONDS.sleep(sysConfig.retryInterval)
          }
        }
      }
    })

  }

  def ackArgs(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield Array[AnyRef](id.asInstanceOf[AnyRef], status, message)
  }

}
