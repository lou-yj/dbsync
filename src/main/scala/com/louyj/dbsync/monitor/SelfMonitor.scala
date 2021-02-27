package com.louyj.dbsync.monitor

import com.louyj.dbsync.DatasourcePools
import com.louyj.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.dbsync.sync.ComponentManager
import io.javalin.Javalin
import org.slf4j.LoggerFactory

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/27
 *
 */
class SelfMonitor(sysConfig: SysConfig,
                  dbConfigs: List[DatabaseConfig],
                  dsPools: DatasourcePools,
                  componentManager: ComponentManager) {

  val logger = LoggerFactory.getLogger(getClass)
  logger.info(s"Endpoints listen on ${sysConfig.endpointPort}")
  val app = Javalin.create(config => {
    config.showJavalinBanner = false
  }).start(sysConfig.endpointPort)
  new Endpoints(app, sysConfig, dbConfigs, dsPools, componentManager)

  
}
