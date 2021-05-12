package com.louyj.dbsync.sync

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.component.HourStatisticsComponent
import com.louyj.dbsync.component.state.QueueManager
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * Create at 2020/8/25 9:53<br/>
 *
 * @author Louyj<br/>
 */

class ErrorResolver(queueManager: QueueManager, ctx: SystemContext)
  extends Thread with HourStatisticsComponent {

  val logger = LoggerFactory.getLogger(getClass)

  setName("error-resolver")
  start()

  override def run(): Unit = {
    logger.info("Error resolver worker lanuched")
    while (ctx.running) {
      heartbeat()
      try {
        val errorBatch = queueManager.takeError(this)
        loopRetry(errorBatch)
        incr(errorBatch.args.size)
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
    val srcJdbc = ctx.dsPools.jdbcTemplate(sourceDb)
    val tarJdbc = ctx.dsPools.jdbcTemplate(targetDb)
    (errorBatch.args zip errorBatch.ids zip errorBatch.hashs).foreach(triple => {
      val retry = new AtomicInteger(ctx.sysConfig.maxRetry)
      val tuple = triple._1
      val args = tuple._1
      val id = tuple._2
      val hash = triple._2
      val partition = math.abs(hash % ctx.sysConfig.partition).intValue()
      val dbConfig = ctx.dbConfigsMap(sourceDb)
      val dbOpt = dbOpts(dbConfig.`type`);
      logger.info(s"Retry data ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id, will retry ${ctx.sysConfig.maxRetry} times interval ${ctx.sysConfig.retryInterval}ms")
      while (retry.decrementAndGet() > 0) {
        try {
          tarJdbc.update(errorBatch.sql, args: _*)
          val ackSql = dbOpt.batchAck(srcJdbc, dbConfig.sysSchema, List(id), "OK", s"success with ${ctx.sysConfig.maxRetry - retry.get()} retry")
          logger.info(s"Successfully retry for ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id, total retry ${ctx.sysConfig.maxRetry - retry.get()}")
          queueManager.resolvedError(partition, hash, id)
          retry.set(0)
        } catch {
          case e: InterruptedException => throw e
          case e: Exception => {
            val message = s"Retry failed ${errorBatch.schema}.${errorBatch.table}[$sourceDb->$targetDb] id $id, total retry ${ctx.sysConfig.maxRetry - retry.get()}, reason ${e.getClass.getSimpleName}-${e.getMessage}"
            logger.warn(message)
            dbOpt.batchAck(srcJdbc, dbConfig.sysSchema, List(id), "ERR", message)
            TimeUnit.MILLISECONDS.sleep(ctx.sysConfig.retryInterval)
          }
        }
      }
    })

  }

  def ackArgs(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield Array[AnyRef](id.asInstanceOf[AnyRef], status, message)
  }

  override def heartbeatInterval(): Long = TimeUnit.MINUTES.toMillis(2)

}
