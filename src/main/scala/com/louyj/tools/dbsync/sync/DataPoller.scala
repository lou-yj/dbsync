package com.louyj.tools.dbsync.sync

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.HashBasedTable
import com.google.common.hash.Hashing
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig, SysConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 17:46<br/>
 *
 * @author Louyj<br/>
 */

class DataPoller(sysConfig: SysConfig, dbConfig: DatabaseConfig,
                 jdbc: JdbcTemplate, queueManager: QueueManager,
                 syncConfigs: Map[String, SyncConfig]) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  val objectMapper = new ObjectMapper()
  objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
  objectMapper.registerModule(DefaultScalaModule)

  var startTime: Timestamp = _
  var endTime: Timestamp = _

  setName(s"poller-${dbConfig.name}")
  start()


  override def run(): Unit = {
    val dbOpt = DbOperationRegister.dbOpts(dbConfig.`type`)
    val maxPollWait = sysConfig.maxPollWait
    val batchSize = sysConfig.batch
    logger.info("Start data poller for database {}", dbConfig.name)
    while (!isInterrupted) {
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
  }

  def pushModel(model: SyncDataModel, dataTable: HashBasedTable[String, Int, ListBuffer[SyncData]]): Unit = {
    val syncKey = s"${model.sourceDb}:${model.schema}:${model.table}"
    if (!syncConfigs.contains(syncKey)) {
      logger.warn(s"No such sync table $syncKey")
      return ()
    }
    val targetDb = model.targetDb
    val syncConfig = syncConfigs(syncKey)
    val schema = if (syncConfig.targetSchema == null) model.schema else syncConfig.targetSchema
    val table = if (syncConfig.targetTable == null) model.table else syncConfig.targetTable
    val keys = syncConfig.sourceKeys.split(",")
    val data = objectMapper.readValue(model.data, classOf[Map[String, AnyRef]])
    val keyValues = (for (item <- keys) yield data.getOrElse(item, "")).mkString(":")
    val partitionKey = s"$schema:$table:$keyValues"
    val hash = Hashing.murmur3_128().newHasher().putString(partitionKey, StandardCharsets.UTF_8).hash().asLong()
    val syncData = SyncData(hash, model.id, model.operation, schema, table, keys, data)
    val partition = math.abs(hash % sysConfig.partition).intValue
    var listBuffer = dataTable.get(targetDb, partition)
    if (listBuffer == null) {
      listBuffer = new ListBuffer[SyncData]
      dataTable.put(targetDb, partition, listBuffer)
    }
    listBuffer += syncData
  }


}
