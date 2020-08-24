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
                          var maxPoolSize: Int = 15)


case class SyncConfig(sourceDb: String, targetDb: String,
                      sourceSchema: String, sourceTable: String,
                      sourceKeys: String,
                      var targetSchema: String, var targetTable: String,
                      var insertCondition: String, var updateCondition: String, var deleteCondition: String)

case class SysConfig(var batch: Int = 10000, var partition: Int = 10,
                     var maxPollWait: Long = 60000,
                     var cleanInterval: Long = 3600000,
                     var dataKeepHours: Int = 24)

case class DbContext(queueManager: QueueManager,
                     dbConfig: DatabaseConfig,
                     dbConfigs: Map[String, DatabaseConfig],
                     syncConfigs: Map[String, SyncConfig],
                     jdbcTemplate: JdbcTemplate,
                     dsPools: DatasourcePools,
                     dbOpts: Map[String, DbOperation],
                     sysConfig: SysConfig
                    )

