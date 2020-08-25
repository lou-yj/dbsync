package com.louyj.tools.dbsync.sync

import java.util.concurrent.TimeUnit

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.DatabaseConfig
import com.louyj.tools.dbsync.dbopt.DbOperation
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 19:07<br/>
 *
 * @author Louyj<br/>
 */

class DataSyncer(dbConfigs: Map[String, DatabaseConfig],
                 queueManager: QueueManager, dsPools: DatasourcePools) {

  for (partition <- 0 until queueManager.partition) {
    val sendWorker = new SyncWorker(partition, dbConfigs, queueManager, dsPools)
    sendWorker.start()
  }
}

class SyncWorker(partition: Int,
                 dbConfigs: Map[String, DatabaseConfig],
                 queueManager: QueueManager, dsPools: DatasourcePools) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  setName(s"sync-$partition")

  override def run(): Unit = {
    logger.info(s"Start sync worker $getName")
    while (!isInterrupted) {
      try {
        val batchData = queueManager.take(partition)
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
        val dbOpt = dbOpts(targetDbConfig.`type`)
        val sourceSysSchema = sourceDbConfig.sysSchema
        batchData.items.foreach(syncData => {
          val sqlTuple = toSql(dbOpt, syncData)
          if (preSql == sqlTuple._1) {
            preArgs += sqlTuple._2
            preIds += syncData.id
            preHashs += syncData.hash
          } else {
            exec(sourceDb, targetDb, sourceSysSchema,
              dbOpt, srcJdbc, tarJdbc,
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
          exec(sourceDb, targetDb, sourceSysSchema,
            dbOpt, srcJdbc, tarJdbc,
            preSchema, preTable, preSql, preArgs.toList, preIds.toList, preHashs.toList)
        }
      } catch {
        case e: InterruptedException => throw e
        case e: Exception =>
          logger.error("Sync failed", e)
          TimeUnit.SECONDS.sleep(1)
      }
    }
  }

  def toSql(dbOpts: DbOperation, syncData: SyncData): (String, Array[AnyRef]) = {
    syncData.operation match {
      case "I" | "U" =>
        val fieldBuffer = new ListBuffer[String]
        val valueBuffer = new ListBuffer[AnyRef]
        val conflictSetBuffer = new ListBuffer[AnyRef]
        syncData.data.foreach(item => {
          fieldBuffer += s"""\"${item._1}\""""
          valueBuffer += item._2
          if (!syncData.key.contains(item._1)) {
            conflictSetBuffer += s"""\"${item._1}\" = EXCLUDED.\"${item._1}\""""
          }
        })
        val sql = dbOpts.batchUpsertSql(syncData, fieldBuffer, valueBuffer, conflictSetBuffer)
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

  def exec(sourceDb: String, targetDb: String,
           sourceSysSchema: String,
           dbOpts: DbOperation, srcJdbc: JdbcTemplate, tarJdbc: JdbcTemplate,
           schema: String, table: String,
           sql: String, args: List[Array[AnyRef]],
           ids: List[Long], hashs: List[Long]): Unit = {
    if (sql == null) return ()
    import scala.collection.JavaConverters._
    try {
      tarJdbc.batchUpdate(sql, args.asJava)
      val ackSql = dbOpts.batchAckSql(sourceSysSchema)
      srcJdbc.batchUpdate(ackSql, ackArgs(ids, "OK", "").asJava)
      logger.info(s"Sync ${args.size} data for table $schema.$table[$targetDb]")
    } catch {
      case e: InterruptedException => throw e
      case e: Exception =>
        val reason = s"${e.getClass.getSimpleName}-${e.getMessage}"
        fallbackExec(sourceDb, targetDb, sourceSysSchema,
          dbOpts, srcJdbc, table, args, ids, hashs, reason)
    }
  }

  def fallbackExec(sourceDb: String, targetDb: String,
                   sourceSysSchema: String,
                   dbOpts: DbOperation, tarJdbc: JdbcTemplate,
                   preTable: String, args: List[Array[AnyRef]], ids: List[Long], hashs: List[Long], reason: String): Unit = {
    logger.warn(s"Failed sync ${args.size} data for table $preTable, reason $reason")
    val ackSql = dbOpts.batchAckSql(sourceSysSchema)
    import scala.collection.JavaConverters._
    tarJdbc.batchUpdate(ackSql, ackArgs(ids, "ERR", reason).asJava)

    val errorBatch = ErrorBatch(sourceDb, targetDb, preTable, args, ids, hashs, reason)
    queueManager.putError(errorBatch)
  }

  def ackArgs(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield Array[AnyRef](id.asInstanceOf[AnyRef], status, message)
  }

}