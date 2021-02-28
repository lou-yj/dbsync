package com.louyj.dbsync

import com.louyj.dbsync.SystemStatus.{GREEN, SystemStatus}
import com.louyj.dbsync.config.ConfigParser
import org.joda.time.DateTime

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/28
 *
 */
class SystemContext(configParser: ConfigParser, val dsPools: DatasourcePools) {

  val uptime = DateTime.now.toString("yyyy-MM-dd HH:mm:ss")
  var running: Boolean = true
  var status: SystemStatus = GREEN
  var restartReason: String = "N/A"

  val sysConfig = configParser.sysConfig
  val dbConfigs = configParser.databaseConfig
  val dbConfigsMap = configParser.databaseConfigMap
  val syncConfigs = configParser.syncConfig
  val syncConfigsMap = configParser.syncConfigMap

}

object SystemStatus extends Enumeration {

  type SystemStatus = Value

  val GREEN = Value("GREEN")
  val YELLOW = Value("YELLOW")
  val RED = Value("RED")

}
