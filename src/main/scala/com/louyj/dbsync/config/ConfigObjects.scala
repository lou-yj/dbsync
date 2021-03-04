package com.louyj.dbsync.config

/**
 *
 * Create at 2020/8/23 15:44<br/>
 *
 * @author Louyj<br/>
 */


case class AppConfig(
                      sys: SysConfig,
                      db: List[DatabaseConfig],
                      sync: List[SyncConfig],
                      monitor: List[MonitorConfig]
                    )

case class DatabaseConfig(name: String, sysSchema: String, `type`: String,
                          driver: String, url: String,
                          user: String, password: String,
                          var maxPoolSize: Int = 15,
                          var createIndex: Boolean = true)


case class SyncConfig(sourceDb: String, targetDb: String,
                      sourceSchema: String, sourceTable: String,
                      sourceKeys: String,
                      var targetSchema: String, var targetTable: String,
                      var insertCondition: String, var updateCondition: String, var deleteCondition: String)

case class SysConfig(var batch: Int = 10000,
                     var partition: Int = 10,
                     var maxPollWait: Long = 60000,
                     var cleanInterval: Long = 3600000,
                     var syncTriggerInterval: Long = 1800000,
                     var dataKeepHours: Int = 24,
                     var maxRetry: Int = Int.MaxValue,
                     var retryInterval: Int = 10000,
                     var pollBlockInterval: Long = 1000,
                     var workDirectory: String = ".",
                     var stateDirectory: String = "state",
                     var endpointPort: Int = 8080)


case class MonitorRule(
                        var heartbeatLostOver: Long = -1L,
                        var syncBlockedOver: Long = -1L,
                        var syncErrorOver: Long = -1L,
                        var syncPendingOver: Long = -1L
                      )

case class MonitorConfig(
                          alarm: MonitorRule,
                          alarmType: String,
                          alarmArgs: Map[String, Object],
                          restart: MonitorRule
                        )