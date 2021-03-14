package com.louyj.dbsync.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * @Author: Louyj
 * @Date: Created at 2021/3/14
 *
 */
object JsonUtils {

  def jackson() = {
    val jackson = new ObjectMapper()
    jackson.configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
    jackson.configure(ALLOW_SINGLE_QUOTES, true)
    jackson.registerModule(DefaultScalaModule)
  }

  def jacksonWithFieldAccess() = {
    val jackson = new ObjectMapper()
    jackson.configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
    jackson.configure(ALLOW_SINGLE_QUOTES, true)
    jackson.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
    jackson.registerModule(DefaultScalaModule)
  }

  def yaml() = {
    val yaml = new ObjectMapper(new YAMLFactory)
    yaml.registerModule(DefaultScalaModule)
  }

}
