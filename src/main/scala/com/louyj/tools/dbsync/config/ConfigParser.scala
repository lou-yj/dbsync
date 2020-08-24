package com.louyj.tools.dbsync.config

import java.io.InputStream

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.yaml.snakeyaml.Yaml

/**
 *
 * Create at 2020/8/23 15:13<br/>
 *
 * @author Louyj<br/>
 */

class ConfigParser(stream: InputStream) {

  val configMap: Map[String, Any] = new Yaml().load(stream)
  val objectMapper = new ObjectMapper()
  objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
  objectMapper.registerModule(DefaultScalaModule)

  def sysConfig = {
    val sysMap = configMap("sys")
    objectMapper.convertValue(sysMap, classOf[SysConfig])
  }

  def databaseConfig = {
    for (item <- configMap("db").asInstanceOf[List[Map[String, Any]]]) yield objectMapper.convertValue(item, classOf[DatabaseConfig])
  }

  def syncConfig = {
    for (item <- configMap("sync").asInstanceOf[List[Map[String, Any]]])
      yield objectMapper.convertValue(item, classOf[SyncConfig])
  }

  def syncConfigMap = {
    (for (item <- configMap("sync").asInstanceOf[List[Map[String, Any]]])
      yield {
        val config = objectMapper.convertValue(item, classOf[SyncConfig])
        s"${config.sourceDb}:${config.sourceSchema}:${config.sourceTable}" -> config
      }).toMap
  }

}
