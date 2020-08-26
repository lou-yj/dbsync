package com.louyj.tools.dbsync.dbopt

import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig, SysConfig}
import com.louyj.tools.dbsync.sync.{SyncData, SyncDataModel}
import org.springframework.jdbc.core.JdbcTemplate

/**
 *
 * Create at 2020/8/24 9:43<br/>
 *
 * @author Louyj<br/>
 */

trait DbOperation {

  def name(): String

  def pollBatch(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, batch: Int, offset: Long): List[SyncDataModel]

  def prepareBatchUpsert(syncData: SyncData): (String, Array[AnyRef])

  def prepareBatchDelete(syncData: SyncData): (String, Array[AnyRef])


  def buildInsertTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildUpdateTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildDeleteTrigger(dbName: String, sysSchema: String, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildSysTable(dbName: String, jdbcTemplate: JdbcTemplate,
                    sysSchema: String): Unit

  def cleanSysTable(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, keepHours: Int): Int

  def cleanBlockedStatus(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, sysConfig: SysConfig): Int


  def tableExists(jdbcTemplate: JdbcTemplate,
                  schema: String, table: String): Boolean

  def uniqueIndexExists(jdbcTemplate: JdbcTemplate,
                        schema: String, table: String, indexColumns: String): Boolean

  def batchAck(jdbcTemplate: JdbcTemplate, sysSchema: String, ids: List[Long], status: String, message: String = ""): Array[Int]
}


object DbOperationRegister {

  val pgOpt = new PgOperation

  val dbOpts = Map(pgOpt.name() -> pgOpt)

}
