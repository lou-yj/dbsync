package com.louyj.dbsync.sync

import com.louyj.dbsync.DatasourcePools
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.dbopt.DbOperation
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 19:07<br/>
 *
 * @author Louyj<br/>
 */

class DataSyncer(dbConfigs: Map[String, DatabaseConfig],
                 queueManager: QueueManager, dsPools: DatasourcePools) {

  val sendWorkers = (for (partition <- 0 until queueManager.partition) yield {
    val sendWorker = new SyncWorker(partition, dbConfigs, queueManager, dsPools)
    sendWorker.start()
    sendWorker
  }).toList


}

class SyncWorker(partition: Int,
                 dbConfigs: Map[String, DatabaseConfig],
                 queueManager: QueueManager, dsPools: DatasourcePools)
  extends HourStatisticsComponent {

  val logger = LoggerFactory.getLogger(getClass)

  setName(s"sync-$partition")

  override def run(): Unit = {
    logger.info(s"Start sync worker $getName")
    while (!isInterrupted) {
      heartbeat
      try {
        syncBlocked()
        val batchData = queueManager.take(partition)
        syncData(batchData)
      } catch {
        case e: InterruptedException => throw e
        case e: Exception =>
          logger.error("Sync failed", e)
          TimeUnit.SECONDS.sleep(1)
      }
    }
    logger.info(s"Stop sync worker $getName")
  }

  def syncBlocked() = {
    val blockedDatas = queueManager.pollBlocked(partition)
    if (blockedDatas != null) {
      blockedDatas.foreach(blockedData => {
        val data = blockedData.data
        val head = data.items.head
        logger.info(s"Resume blocked data for table ${head.schema}.${head.table}[${data.sourceDb}->${data.targetDb}] id ${head.id}")
        syncData(data)
      })
    }
  }

  def syncData(batchData: BatchData) = {
    if (batchData != null) {
      val targetDb = batchData.targetDb
      val sourceDb = batchData.sourceDb
      val tarJdbc = dsPools.jdbcTemplate(targetDb)
      val srcJdbc = dsPools.jdbcTemplate(sourceDb)
      var preSchema: String = null
      var preTable: String = null
      var preSql: String = null
      val preArgs = new ListBuffer[Array[AnyRef]]
      val preIds = new ListBuffer[Long]
      val preHashs = new ListBuffer[Long]
      val targetDbConfig = dbConfigs(targetDb)
      val sourceDbConfig = dbConfigs(sourceDb)
      val srcDbOpt = dbOpts(sourceDbConfig.`type`)
      val tarDbopt = dbOpts(targetDbConfig.`type`)
      val sourceSysSchema = sourceDbConfig.sysSchema
      var failedStatus = false
      batchData.items.foreach(syncData => {
        val sqlTuple = toSql(tarDbopt, syncData)
        if (preSql == sqlTuple._1) {
          preArgs += sqlTuple._2
          preIds += syncData.id
          preHashs += syncData.hash
        } else {
          failedStatus = exec(failedStatus, sourceDb, targetDb, sourceSysSchema,
            srcDbOpt, srcJdbc, tarJdbc,
            preSchema, preTable, preSql, preArgs.toList, preIds.toList, preHashs.toList)
          preSchema = syncData.schema
          preTable = syncData.table
          preSql = sqlTuple._1
          preArgs.clear()
          preArgs += sqlTuple._2
          preIds.clear()
          preIds += syncData.id
          preHashs.clear()
          preHashs += syncData.hash
        }
      })
      if (preArgs.nonEmpty) {
        exec(failedStatus, sourceDb, targetDb, sourceSysSchema,
          srcDbOpt, srcJdbc, tarJdbc,
          preSchema, preTable, preSql, preArgs.toList, preIds.toList, preHashs.toList)
      }
    }
  }

  def toSql(tarDbOpts: DbOperation, syncData: SyncData): (String, Array[AnyRef]) = {
    syncData.operation match {
      case "I" | "U" =>
        tarDbOpts.prepareBatchUpsert(syncData)
      case "D" =>
        tarDbOpts.prepareBatchDelete(syncData)
    }
  }

  def exec(failedStatus: Boolean, sourceDb: String, targetDb: String,
           sourceSysSchema: String,
           srcDbOpt: DbOperation, srcJdbc: JdbcTemplate, tarJdbc: JdbcTemplate,
           schema: String, table: String,
           sql: String, args: List[Array[AnyRef]],
           ids: List[Long], hashs: List[Long]): Boolean = {
    if (sql == null) return failedStatus
    import scala.collection.JavaConverters._
    if (failedStatus) {
      val reason = s"Batch failed due to previous error"
      logger.error(reason)
      fallbackExec(sourceDb, targetDb, sourceSysSchema,
        srcDbOpt, srcJdbc, schema, table, sql, args, ids, hashs, reason)
      return failedStatus
    }
    try {
      tarJdbc.batchUpdate(sql, args.asJava)
      srcDbOpt.batchAck(srcJdbc, sourceSysSchema, ids, "OK")
      logger.info(s"Sync ${args.size} data for table $schema.$table[$sourceDb->$targetDb]")
      incr(args.size)
      failedStatus && true
    } catch {
      case e: InterruptedException => throw e
      case e: Exception =>
        logger.error("Sync data failed", e)
        val reason = s"${e.getClass.getSimpleName}-${e.getMessage}"
        fallbackExec(sourceDb, targetDb, sourceSysSchema,
          srcDbOpt, srcJdbc, schema, table, sql, args, ids, hashs, reason)
        failedStatus && false
    }
  }

  def fallbackExec(sourceDb: String, targetDb: String,
                   sourceSysSchema: String,
                   srcDbOpts: DbOperation, srcJdbc: JdbcTemplate,
                   schema: String, table: String,
                   sql: String, args: List[Array[AnyRef]],
                   ids: List[Long], hashs: List[Long], reason: String): Unit = {
    logger.warn(s"Failed sync ${args.size} data for table $schema.$table[$sourceDb->$targetDb], reason $reason")
    srcDbOpts.batchAck(srcJdbc, sourceSysSchema, ids, "ERR", reason)

    val errorBatch = ErrorBatch(sourceDb, targetDb, schema, table, sql, args, ids, hashs, reason)
    queueManager.putError(partition, errorBatch)
  }

  override def heartbeatInterval(): Long = TimeUnit.MINUTES.toMillis(2)
}