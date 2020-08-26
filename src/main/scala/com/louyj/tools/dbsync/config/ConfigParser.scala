package com.louyj.tools.dbsync.config

import java.io.InputStream
import java.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils.isBlank
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

/**
 *
 * Create at 2020/8/23 15:13<br/>
 *
 * @author Louyj<br/>
 */

class ConfigParser(stream: InputStream) {

  val configHashMap: util.HashMap[String, Any] = new Yaml().load(stream)
  val configMap: Map[String, Any] = configHashMap.asScala.toMap
  val objectMapper = new ObjectMapper()
  objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
  objectMapper.registerModule(DefaultScalaModule)

  def sysConfig = {
    val sysMap = configMap("sys")
    validateSysConfig(objectMapper.convertValue(sysMap, classOf[SysConfig]))
  }

  def databaseConfig = {
    for (item <- configMap("db").asInstanceOf[util.List[util.Map[String, Any]]].asScala.toList)
      yield validateDbConfig(objectMapper.convertValue(item, classOf[DatabaseConfig]))
  }

  def databaseConfigMap = {
    (for (item <- configMap("db").asInstanceOf[util.List[util.Map[String, Any]]].asScala.toList)
      yield {
        val config = validateDbConfig(objectMapper.convertValue(item, classOf[DatabaseConfig]))
        config.name -> config
      }).toMap
  }

  def syncConfig = {
    for (item <- configMap("sync").asInstanceOf[util.List[util.Map[String, Any]]].asScala.toList)
      yield validateSyncConfig(objectMapper.convertValue(item, classOf[SyncConfig]))
  }

  def syncConfigMap = {
    (for (item <- configMap("sync").asInstanceOf[util.List[util.Map[String, Any]]].asScala.toList)
      yield {
        val config = objectMapper.convertValue(item, classOf[SyncConfig])
        s"${config.sourceDb}:${config.sourceSchema}:${config.sourceTable}" -> validateSyncConfig(config)
      }).toMap
  }

  def validateSyncConfig(syncConfig: SyncConfig) = {
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

  def validateDbConfig(dbConfig: DatabaseConfig) = {
    if (isBlank(dbConfig.name)) throw new RuntimeException("db config error, missing name")
    if (isBlank(dbConfig.`type`)) throw new RuntimeException("db config error, missing type")
    if (isBlank(dbConfig.driver)) throw new RuntimeException("db config error, missing driver")
    if (isBlank(dbConfig.url)) throw new RuntimeException("db config error, missing url")
    if (isBlank(dbConfig.user)) throw new RuntimeException("db config error, missing user")
    if (isBlank(dbConfig.password)) throw new RuntimeException("db config error, missing password")
    if (isBlank(dbConfig.sysSchema)) throw new RuntimeException("db config error, missing sysSchema")
    if (dbConfig.maxPoolSize == 0) dbConfig.maxPoolSize = 15
    dbConfig
  }

  def validateSysConfig(sysConfig: SysConfig) = {
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
    if (isBlank(sysConfig.stateDirectory)) sysConfig.stateDirectory = "status"
    sysConfig
  }

}
