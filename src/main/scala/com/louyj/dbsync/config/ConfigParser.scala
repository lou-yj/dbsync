package com.louyj.dbsync.config

import com.louyj.dbsync.util.JsonUtils
import org.apache.commons.lang3.StringUtils.isBlank

import java.io.InputStream

/**
 *
 * Create at 2020/8/23 15:13<br/>
 *
 * @author Louyj<br/>
 */

class ConfigParser(stream: InputStream) {

  private val yaml = JsonUtils.yaml()

  val appConfig: AppConfig = yaml.readValue(stream, classOf[AppConfig])

  def sysConfig: SysConfig = {
    validateSysConfig(appConfig.sys)
  }

  def databaseConfig: List[DatabaseConfig] = {
    for (item <- appConfig.db) yield validateDbConfig(item)
  }

  def databaseConfigMap: Map[String, DatabaseConfig] = {
    (for (item <- appConfig.db) yield item.name -> validateDbConfig(item)).toMap
  }

  def syncConfig: List[SyncConfig] = {
    for (item <- appConfig.sync) yield validateSyncConfig(item)
  }

  def syncConfigMap: Map[String, SyncConfig] = {
    (for (item <- appConfig.sync)
      yield s"${item.sourceDb}:${item.sourceSchema}:${item.sourceTable}" -> validateSyncConfig(item)).toMap
  }

  def validateSyncConfig(syncConfig: SyncConfig): SyncConfig = {
    if (isBlank(syncConfig.sourceDb)) throw new RuntimeException("sync config error, missing sourceDb")
    if (isBlank(syncConfig.targetDb)) throw new RuntimeException("sync config error, missing targetDb")
    if (isBlank(syncConfig.sourceSchema)) throw new RuntimeException("sync config error, missing sourceSchema")
    if (isBlank(syncConfig.sourceTable)) throw new RuntimeException("sync config error, missing sourceTable")
    if (isBlank(syncConfig.sourceKeys)) throw new RuntimeException("sync config error, missing sourceKeys")
    if (isBlank(syncConfig.targetSchema)) syncConfig.targetSchema = syncConfig.sourceSchema
    if (isBlank(syncConfig.targetTable)) syncConfig.targetTable = syncConfig.sourceTable
    if (isBlank(syncConfig.insertCondition)) syncConfig.insertCondition = "1=1"
    if (isBlank(syncConfig.updateCondition)) syncConfig.updateCondition = "1=1"
    if (isBlank(syncConfig.deleteCondition)) syncConfig.deleteCondition = "1=1"
    syncConfig
  }

  def validateDbConfig(dbConfig: DatabaseConfig): DatabaseConfig = {
    if (isBlank(dbConfig.name)) throw new RuntimeException("db config error, missing name")
    if (isBlank(dbConfig.`type`)) throw new RuntimeException("db config error, missing type")
    if (isBlank(dbConfig.driver)) throw new RuntimeException("db config error, missing driver")
    if (isBlank(dbConfig.url)) throw new RuntimeException("db config error, missing url")
    if (isBlank(dbConfig.user)) throw new RuntimeException("db config error, missing user")
    if (isBlank(dbConfig.password)) throw new RuntimeException("db config error, missing password")
    if (isBlank(dbConfig.sysSchema)) throw new RuntimeException("db config error, missing sysSchema")
    if (dbConfig.maxPoolSize == 0) dbConfig.maxPoolSize = 15
    if (dbConfig.queryTimeout == 0) dbConfig.queryTimeout = 1800000
    if (dbConfig.maxWaitTime == 0) dbConfig.maxWaitTime = 60000
    dbConfig
  }

  def validateSysConfig(sysConfig: SysConfig): SysConfig = {
    if (sysConfig.batch == 0) sysConfig.batch = 10000
    if (sysConfig.partition == 0) sysConfig.partition = 100
    if (sysConfig.maxPollWait == 0) sysConfig.maxPollWait = 60000
    if (sysConfig.cleanInterval == 0) sysConfig.cleanInterval = 3600000
    if (sysConfig.dataKeepHours == 0) sysConfig.dataKeepHours = 24
    if (sysConfig.maxRetry == 0) sysConfig.maxRetry = Int.MaxValue
    if (sysConfig.retryInterval == 0) sysConfig.retryInterval = 10000
    if (sysConfig.pollBlockInterval == 0) sysConfig.pollBlockInterval = 1000
    if (sysConfig.syncTriggerInterval == 0) sysConfig.syncTriggerInterval = 1800000
    if (isBlank(sysConfig.workDirectory)) sysConfig.workDirectory = "."
    if (isBlank(sysConfig.stateDirectory)) sysConfig.stateDirectory = "state"
    if (sysConfig.endpointPort == 0) sysConfig.endpointPort = 8080
    sysConfig
  }

}
