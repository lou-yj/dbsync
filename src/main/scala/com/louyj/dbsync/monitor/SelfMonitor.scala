package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.sync.ComponentManager
import io.javalin.Javalin
import org.slf4j.LoggerFactory

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/27
 *
 */
class SelfMonitor(componentManager: ComponentManager, ctx: SystemContext) {

  val logger = LoggerFactory.getLogger(getClass)
  logger.info(s"Endpoints listen on ${ctx.sysConfig.endpointPort}")
  val app = Javalin.create(config => {
    config.showJavalinBanner = false
  }).start(ctx.sysConfig.endpointPort)
  new Endpoints(app, componentManager, ctx)


}
