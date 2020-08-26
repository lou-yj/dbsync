package com.louyj.tools.dbsync.dbopt

import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig, SysConfig}
import com.louyj.tools.dbsync.sync.{BlockedData, SyncData, SyncDataModel}
import org.springframework.jdbc.core.JdbcTemplate

import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/24 9:43<br/>
 *
 * @author Louyj<br/>
 */

trait DbOperation {

  def name(): String

  def pollBatch(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, batch: Int, offset: Long): List[SyncDataModel]

  def batchUpsertSql(syncData: SyncData, fieldBuffer: ListBuffer[String], valueBuffer: ListBuffer[AnyRef], conflictSetBuffer: ListBuffer[AnyRef]): String

  def batchDeleteSql(syncData: SyncData, whereBuffer: ListBuffer[String]): String

  def batchAckSql(sysSchema: String): String

  def buildInsertTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildUpdateTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildDeleteTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildSysTable(dbName: String, jdbcTemplate: JdbcTemplate,
                    sysSchema: String): Unit

  def cleanSysTable(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, keepHours: Int): Int

  def cleanBlockedStatus(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, sysConfig: SysConfig): Int

  def markBlocked(schema: String, jdbcTemplate: JdbcTemplate, data: BlockedData, partition: Int): Int

  def tableExists(jdbcTemplate: JdbcTemplate,
                  schema: String, table: String): Boolean

  def uniqueIndexExists(jdbcTemplate: JdbcTemplate,
                        schema: String, table: String, indexColumns: String): Boolean
}


object DbOperationRegister {

  val pgOpt = new PgOperation

  val dbOpts = Map(pgOpt.name() -> pgOpt)

}
