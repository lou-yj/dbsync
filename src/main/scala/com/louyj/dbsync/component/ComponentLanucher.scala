package com.louyj.dbsync.component

import com.louyj.dbsync.SystemContext
import org.slf4j.LoggerFactory

import java.lang.Thread.currentThread
import java.util.ServiceLoader
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * @Author: Louyj
 * @Date: Created at 2021/5/13
 *
 */
class ComponentLanucher(ctx: SystemContext) extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(getClass)
  private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(10)

  val components: List[Component] = ServiceLoader.load(classOf[Component], currentThread.getContextClassLoader)
    .iterator().asScala.toList.sortBy(_.order())

  components.foreach(c => {
    val config = ctx.appConfig.component(c.name())
    if (c.isInstanceOf[ComponentConfigAware]) {
      c.asInstanceOf[ComponentConfigAware].withConfig(config)
    }
    if (c.isInstanceOf[EndpointsComponent]) {
      c.asInstanceOf[EndpointsComponent].withJavalin(ctx.app)
    }
    c.open(ctx)
  }
  )

  components.foreach(_ match {
    case c: ScheduleComponent =>
      logger.info(s"Start ${c.name()} component, scheduled at fixed rate of ${c.rate()}ms")
      executorService.scheduleAtFixedRate(c, c.delay(), c.rate(), TimeUnit.MILLISECONDS)
  })

  override def close(): Unit = {
    components.foreach(_.close)
  }
}
