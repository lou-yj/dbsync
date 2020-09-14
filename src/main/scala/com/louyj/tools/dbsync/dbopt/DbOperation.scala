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

  def pollBatch(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, batch: Int): List[SyncDataModel]

  def prepareBatchUpsert(syncData: SyncData): (String, Array[AnyRef])

  def prepareBatchDelete(syncData: SyncData): (String, Array[AnyRef])


  def buildInsertTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildUpdateTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildDeleteTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig)

  def buildSysTable(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate): Unit

  def cleanSysTable(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, keepHours: Int): Int

  def buildBootstrapState(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, sysConfig: SysConfig): Int


  def tableExists(jdbcTemplate: JdbcTemplate, schema: String, table: String): Boolean

  def uniqueIndexExists(jdbcTemplate: JdbcTemplate, schema: String, table: String, indexColumns: String): Boolean

  def batchAck(jdbcTemplate: JdbcTemplate, sysSchema: String, ids: List[Long], status: String, message: String = ""): Array[Int]
}



