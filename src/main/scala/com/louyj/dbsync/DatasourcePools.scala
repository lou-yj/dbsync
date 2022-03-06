package com.louyj.dbsync

import com.alibaba.druid.pool.DruidDataSource
import com.louyj.dbsync.config.DatabaseConfig
import org.springframework.jdbc.core.JdbcTemplate

/**
 *
 * Create at 2020/8/23 15:53<br/>
 *
 * @author Louyj<br/>
 */

class DatasourcePools(databaseConfigs: List[DatabaseConfig]) {

  val jdbcTpls = (for (item <- databaseConfigs) yield {
    val ds = new DruidDataSource
    ds.setDriverClassName(item.driver)
    ds.setUrl(item.url)
    ds.setUsername(item.user)
    ds.setPassword(item.password)
    ds.setMaxActive(item.maxPoolSize)
    ds.setName(item.name)
    ds.setQueryTimeout(item.queryTimeout)
    ds.setMaxWait(item.maxWaitTime * 1000)
    val jdbcTemplate = new JdbcTemplate(ds)
    item.name -> jdbcTemplate
  }).toMap

  val sysJdbcTpls = (for (item <- databaseConfigs) yield {
    val ds = new DruidDataSource
    ds.setDriverClassName(item.driver)
    ds.setUrl(item.url)
    ds.setUsername(item.user)
    ds.setPassword(item.password)
    ds.setMaxActive(item.maxPoolSize)
    ds.setName(item.name + "_sys")
    ds.setQueryTimeout(item.sysQueryTimeout)
    ds.setMaxWait(item.maxWaitTime * 1000)
    val jdbcTemplate = new JdbcTemplate(ds)
    item.name -> jdbcTemplate
  }).toMap

  def jdbcTemplate(name: String) = jdbcTpls(name)

  def sysJdbcTemplate(name: String) = sysJdbcTpls(name)


}
