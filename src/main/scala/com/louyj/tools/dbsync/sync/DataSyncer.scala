package com.louyj.tools.dbsync.sync

import java.util.concurrent.TimeUnit

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, DbContext}
import com.louyj.tools.dbsync.dbopt.DbOperation
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 19:07<br/>
 *
 * @author Louyj<br/>
 */

class DataSyncer(dbContext: DbContext) {

  for (partition <- 0 until dbContext.queueManager.partition) {
    val sendWorker = new SyncWorker(dbContext, partition, dbContext.queueManager, dbContext.dsPools, dbContext.dbConfig)
    sendWorker.start()
  }

}

class SyncWorker(dbContext: DbContext, partition: Int,
                 queueManager: QueueManager, dsPools: DatasourcePools,
                 dbConfig: DatabaseConfig /*, syncConfigs: Map[String, SyncConfig]*/) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  setName(s"sync-${dbConfig.name}-$partition")

  override def run(): Unit = {
    logger.info("Start sync worker for database {} partition {}", dbConfig.name, partition)
    while (!isInterrupted) {
      try {
        val batchData = queueManager.take(partition)
        val jdbcTemplate = dsPools.jdbcTemplate(batchData.targetDb)
        var preTable: String = null
        var preSql: String = null
        val preArgs = new ListBuffer[Array[AnyRef]]
        val preIds = new ListBuffer[Long]
        val dbOpt = dbContext.dbOpts(batchData.targetDb)
        batchData.items.foreach(syncData => {
          val sqlTuple = toSql(dbOpt, batchData.targetDb, syncData)
          if (preSql == sqlTuple._1) {
            preArgs += sqlTuple._2
          } else {
            exec(dbOpt, jdbcTemplate, preTable, preSql, preArgs.toList, preIds.toList)
            preTable = syncData.table
            preSql = sqlTuple._1
            preArgs.clear()
            preArgs += sqlTuple._2
            preIds.clear()
            preIds += syncData.id
          }
        })
        if (preArgs.nonEmpty) {
          exec(dbOpt, jdbcTemplate, preTable, preSql, preArgs.toList, preIds.toList)
        }
      } catch {
        case e: InterruptedException => throw e
        case e: Exception =>
          logger.error("Sync failed", e)
          TimeUnit.SECONDS.sleep(1)
      }
    }
  }

  def toSql(dbOpts: DbOperation, targetDb: String, syncData: SyncData): (String, Array[AnyRef]) = {
    syncData.operation match {
      case "I" =>
        val fieldBuffer = new ListBuffer[String]
        val valueBuffer = new ListBuffer[AnyRef]
        syncData.data.foreach(item => {
          fieldBuffer += s"""\"${item._1}\""""
          valueBuffer += item._2
        })
        val sql = dbOpts.batchInsertSql(syncData, fieldBuffer, valueBuffer)
        (sql, valueBuffer.toArray)
      case "U" =>
        val fieldBuffer = new ListBuffer[String]
        val valueBuffer = new ListBuffer[AnyRef]
        val whereBuffer = new ListBuffer[String]
        val whereValueBuffer = new ListBuffer[AnyRef]
        syncData.data.foreach(item => {
          if (!syncData.key.contains(item._1)) {
            fieldBuffer += s"""\"${item._1}\"=?"""
            valueBuffer += item._2
          } else {
            whereBuffer += s"""\"${item._1}\"=?"""
            whereValueBuffer += item._2
          }
        })
        valueBuffer ++= whereValueBuffer
        val sql = dbOpts.batchUpdateSql(syncData, fieldBuffer, whereBuffer)
        (sql, valueBuffer.toArray)
      case "D" =>
        val whereBuffer = new ListBuffer[String]
        val whereValueBuffer = new ListBuffer[AnyRef]
        syncData.data.foreach(item => {
          if (syncData.key.contains(item._1)) {
            whereBuffer += s"""\"${item._1}\"=?"""
            whereValueBuffer += item._2
          }
        })
        val sql = dbOpts.batchDeleteSql(syncData, whereBuffer)
        (sql, whereValueBuffer.toArray)
    }
  }

  def exec(dbOpts: DbOperation, jdbcTemplate: JdbcTemplate,
           table: String, sql: String, args: List[Array[AnyRef]],
           ids: List[Long]) = {
    import scala.collection.JavaConverters._
    try {
      jdbcTemplate.batchUpdate(sql, args.asJava)
      val ackSql = dbOpts.batchAckSql(dbConfig)
      jdbcTemplate.batchUpdate(ackSql, ackArgs(ids, "OK", "").asJava)
      logger.info("Sync {} data of table {}", args.size, table)
    } catch {
      case e: InterruptedException => throw e
      case e: Exception =>
        val reason = s"${e.getClass.getSimpleName}-${e.getMessage}"
        fallbackExec(dbOpts, jdbcTemplate, table, args, ids, reason)
    }
  }

  def fallbackExec(dbOpts: DbOperation, jdbcTemplate: JdbcTemplate,
                   preTable: String, args: List[Array[AnyRef]],
                   ids: List[Long], reason: String): Unit = {
    logger.warn(s"Failed sync ${args.size} data for table $preTable, reason $reason")
    val ackSql = dbOpts.batchAckSql(dbConfig)
    import scala.collection.JavaConverters._
    jdbcTemplate.batchUpdate(ackSql, ackArgs(ids, "ERR", reason).asJava)
  }

  def ackArgs(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield Array[AnyRef](id.asInstanceOf[AnyRef], status, message)
  }

}