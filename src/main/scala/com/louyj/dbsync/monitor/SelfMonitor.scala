package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.sync.ComponentManager
import org.slf4j.LoggerFactory

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/27
 *
 */
class SelfMonitor(componentManager: ComponentManager, ctx: SystemContext) {

  val logger = LoggerFactory.getLogger(getClass)
  new Endpoints(ctx.app, componentManager, ctx)


}
