package com.louyj.dbsync

import com.alibaba.druid.pool.DruidDataSource
import com.louyj.dbsync.config.ConfigParser
import com.louyj.dbsync.monitor.SyncState
import com.louyj.dbsync.sync.ComponentStatus.{ComponentStatus, GREEN}
import com.louyj.dbsync.sync.StateManger
import io.javalin.Javalin
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/28
 *
 */
class SystemContext(configParser: ConfigParser,
                    val dsPools: DatasourcePools,
                    val restartReason: String) {

  val logger = LoggerFactory.getLogger(getClass)

  val uptime = DateTime.now.toString("yyyy-MM-dd HH:mm:ss")
  var running: Boolean = true
  var componentStatus: ComponentStatus = GREEN
  var syncStatus: SyncState = SyncState("N/A", 0, 0, 0, 0, 0)

  val sysConfig = configParser.sysConfig
  val dbConfigs = configParser.databaseConfig
  val dbConfigsMap = configParser.databaseConfigMap
  val syncConfigs = configParser.syncConfig
  val syncConfigsMap = configParser.syncConfigMap
  val monitorConfig = configParser.appConfig.monitor
  var stateManger: StateManger = _

  logger.info(s"Endpoints listen on ${sysConfig.endpointPort}")
  val app = Javalin.create(config => {
    config.showJavalinBanner = false
  }).start(sysConfig.endpointPort)

  def restart(reason: String): Unit = {
    running = false
    DbSyncLauncher.restart(reason)
  }

  def destroy() = {
    dsPools.jdbcTpls.foreach(e => {
      val dataSource = e._2.getDataSource.asInstanceOf[DruidDataSource]
      try {
        dataSource.close()
        logger.info(s"Datasource ${dataSource.getName} destroyed", e)
      } catch {
        case e: Throwable =>
          logger.error(s"Destroy datasource ${dataSource.getName} failed", e)
      }
    })
    try {
      app.stop()
      logger.info(s"Endpoints ${sysConfig.endpointPort} stopped")
    } catch {
      case e: Throwable =>
        logger.error(s"Stop endpoint ${sysConfig.endpointPort} failed", e)
    }
    try {
      stateManger.blockedMap.close()
      stateManger.retryMap.close()
      stateManger.stateDb.close()
      stateManger.blockedQueue.close()
      stateManger.retryQueue.close()
    } catch {
      case e: Throwable =>
        logger.error(s"Close state db failed", e)
    }
  }

}
