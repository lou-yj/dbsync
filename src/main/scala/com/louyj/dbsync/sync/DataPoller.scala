package com.louyj.dbsync.sync

import com.google.common.collect.HashBasedTable
import com.google.common.hash.Hashing
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.component.HourStatisticsComponent
import com.louyj.dbsync.component.state.QueueManager
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.dbopt.DbOperationRegister
import com.louyj.dbsync.util.JsonUtils
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 17:46<br/>
 *
 * @author Louyj<br/>
 */

class DataPoller(dbConfig: DatabaseConfig,
                 jdbc: JdbcTemplate, queueManager: QueueManager,
                 ctx: SystemContext) extends Thread
  with HourStatisticsComponent {

  val logger = LoggerFactory.getLogger(getClass)

  val objectMapper = JsonUtils.jacksonWithFieldAccess()

  var startTime: Timestamp = _
  var endTime: Timestamp = _

  setName(s"poller-${dbConfig.name}")
  start()


  override def run(): Unit = {
    val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
    val maxPollWait = ctx.sysConfig.maxPollWait
    val batchSize = ctx.sysConfig.batch
    logger.info("Start data poller for database {}", dbConfig.name)
    while (ctx.running) {
      heartbeat
      try {
        val models = dbOpt.pollBatch(jdbc, dbConfig, batchSize)
        if (models.nonEmpty) {
          val dataTable: HashBasedTable[String, Int, ListBuffer[SyncData]] = HashBasedTable.create()
          models.foreach(pushModel(_, dataTable))
          dataTable.cellSet().forEach(c => {
            val batch = BatchData(dbConfig.name, c.getRowKey, c.getColumnKey, c.getValue)
            queueManager.put(c.getColumnKey, batch)
          })
          startTime = models.head.createTime
          endTime = models.last.createTime
          val startTimeStr = new DateTime(startTime.getTime).toString("yyyy-MM-dd HH:mm:ss")
          val endTimeStr = new DateTime(endTime.getTime).toString("yyyy-MM-dd HH:mm:ss")
          logger.info(s"Poll ${models.size} data between $startTimeStr and $endTimeStr, current offset ${models.last.id}")
          incr(models.size)
        }
        val percent = (batchSize - models.size) * 1.0 / batchSize
        val waitTime = (percent * maxPollWait).longValue()
        if (waitTime > 0) {
          logger.debug(s"No enough data, wait $waitTime ms")
          TimeUnit.MILLISECONDS.sleep(waitTime)
        }
      } catch {
        case e: InterruptedException => throw e
        case e: Exception =>
          logger.error("Poll failed", e)
          TimeUnit.SECONDS.sleep(1)
      }
    }
    logger.info("Stop poller for database {}", dbConfig.name)
  }

  def pushModel(model: SyncDataModel, dataTable: HashBasedTable[String, Int, ListBuffer[SyncData]]): Unit = {
    val syncKey = s"${model.sourceDb}:${model.schema}:${model.table}"
    if (!ctx.syncConfigsMap.contains(syncKey)) {
      logger.warn(s"No such sync table $syncKey")
      return ()
    }
    val targetDb = model.targetDb
    val syncConfig = ctx.syncConfigsMap(syncKey)
    val schema = if (syncConfig.targetSchema == null) model.schema else syncConfig.targetSchema
    val table = if (syncConfig.targetTable == null) model.table else syncConfig.targetTable
    val keys = syncConfig.sourceKeys.split(",")
    val data = objectMapper.readValue(model.data, classOf[Map[String, AnyRef]])
    val keyValues = (for (item <- keys) yield data.getOrElse(item, "")).mkString(":")
    val partitionKey = s"$schema:$table:$keyValues"
    val hash = Hashing.murmur3_128().newHasher().putString(partitionKey, StandardCharsets.UTF_8).hash().asLong()
    val syncData = SyncData(hash, model.id, model.operation, schema, table, keys, data)
    val partition = math.abs(hash % ctx.sysConfig.partition).intValue
    var listBuffer = dataTable.get(targetDb, partition)
    if (listBuffer == null) {
      listBuffer = new ListBuffer[SyncData]
      dataTable.put(targetDb, partition, listBuffer)
    }
    listBuffer += syncData
  }

  override def heartbeatInterval(): Long = ctx.sysConfig.maxPollWait
}
