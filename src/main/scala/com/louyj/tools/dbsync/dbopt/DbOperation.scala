package com.louyj.tools.dbsync.dbopt

import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.sync.{SyncData, SyncDataModel}
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

  def batchInsertSql(syncData: SyncData, sourceKeys: String, fieldBuffer: ListBuffer[String], valueBuffer: ListBuffer[AnyRef]): String

  def batchUpdateSql(syncData: SyncData, fieldBuffer: ListBuffer[String], whereBuffer: ListBuffer[String]): String

  def batchDeleteSql(syncData: SyncData, whereBuffer: ListBuffer[String]): String

  def batchAckSql(dbConfig: DatabaseConfig): String

  def buildInsertTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildUpdateTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildDeleteTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildSysTable(dbName: String, jdbcTemplate: JdbcTemplate,
                    sysSchema: String): Unit

}


object DbOperationRegister {

  val pgOpt = new PgOperation

  val dbOpts = Map(pgOpt.name() -> pgOpt)

}
