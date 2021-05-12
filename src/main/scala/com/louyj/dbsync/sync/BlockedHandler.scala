package com.louyj.dbsync.sync

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.component.HourStatisticsComponent
import com.louyj.dbsync.component.state.QueueManager
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 *
 * Create at 2020/8/25 11:13<br/>
 *
 * @author Louyj<br/>
 */

class BlockedHandler(queueManager: QueueManager, ctx: SystemContext)
  extends Thread with HourStatisticsComponent {

  val logger = LoggerFactory.getLogger(getClass)

  setName("blocked-handler")
  start()

  override def run(): Unit = {
    logger.info("Blocked handler worker lanuched")
    while (ctx.running) {
      heartbeat()
      try {
        val blockedData = queueManager.takeBlocked(this)
        val data = blockedData.data
        val sourceDb = data.sourceDb
        val dbConfig = ctx.dbConfigsMap(sourceDb)
        val srcJdbc = ctx.dsPools.jdbcTemplate(sourceDb)
        val hash = data.items.head.hash
        val id = data.items.head.id
        val partition = math.abs(hash % ctx.sysConfig.partition).intValue()
        val dbOpt = dbOpts(dbConfig.`type`)
        logger.warn(s"Data ${id}[$sourceDb] blocked by ${blockedData.blockedBy.mkString(",")}")
        val message = s"partition $partition hash $hash blocked by ${blockedData.blockedBy.mkString(",")}"
        dbOpt.batchAck(srcJdbc, dbConfig.sysSchema, List(id), "BLK", message)
        incr(data.items.size)
      } catch {
        case e: InterruptedException => throw e
        case e: Exception => {
          logger.error("Blocked handler failed", e)
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }
  }

  override def heartbeatInterval(): Long = TimeUnit.MINUTES.toMillis(2)
}
