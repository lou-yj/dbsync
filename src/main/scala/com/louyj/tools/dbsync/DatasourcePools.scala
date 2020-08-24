package com.louyj.tools.dbsync

import com.louyj.tools.dbsync.config.DatabaseConfig
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.springframework.jdbc.core.JdbcTemplate

/**
 *
 * Create at 2020/8/23 15:53<br/>
 *
 * @author Louyj<br/>
 */

class DatasourcePools(databaseConfigs: List[DatabaseConfig]) {

  val jdbcTpls = (for (item <- databaseConfigs) yield {
    val config = new HikariConfig
    config.setJdbcUrl(item.url)
    config.setUsername(item.user)
    config.setPassword(item.password)
    config.setDriverClassName(item.driver)
    config.setMaximumPoolSize(item.maxPoolSize)
    config.setPoolName(item.name)
    val ds = new HikariDataSource(config)
    val jdbcTemplate = new JdbcTemplate(ds)
    item.name -> jdbcTemplate
  }).toMap

  def jdbcTemplate(name: String) = jdbcTpls(name)

}
