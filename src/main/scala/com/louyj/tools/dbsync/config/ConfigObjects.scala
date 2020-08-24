package com.louyj.tools.dbsync.config

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.dbopt.DbOperation
import com.louyj.tools.dbsync.sync.QueueManager
import org.springframework.jdbc.core.JdbcTemplate

/**
 *
 * Create at 2020/8/23 15:44<br/>
 *
 * @author Louyj<br/>
 */

case class DatabaseConfig(name: String, sysSchema: String, `type`: String,
                          driver: String, url: String,
                          user: String, password: String,
                          maxPoolSize: Int = 15)


case class SyncConfig(sourceDb: String, targetDb: String,
                      sourceSchema: String, sourceTable: String,
                      sourceKeys: String,
                      targetSchema: String, targetTable: String,
                      insertCondition: String, updateCondition: String, deleteCondition: String)

case class SysConfig(batch: Int, partition: Int = 10)

case class DbContext(queueManager: QueueManager,
                     dbConfig: DatabaseConfig,
                     syncConfigs: Map[String, SyncConfig],
                     jdbcTemplate: JdbcTemplate,
                     dsPools: DatasourcePools,
                     dbOpts: Map[String, DbOperation],
                     sysConfig: SysConfig
                    )

