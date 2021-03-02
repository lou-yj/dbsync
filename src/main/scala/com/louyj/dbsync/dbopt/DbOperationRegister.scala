package com.louyj.dbsync.dbopt

import java.util.ServiceLoader

/**
 *
 * Create at 2020/8/27 17:12<br/>
 *
 * @author Louyj<br/>
 */

object DbOperationRegister {

  import scala.collection.JavaConverters._

  val dbOpts = ServiceLoader.load(classOf[DbOperation], Thread.currentThread.getContextClassLoader)
    .iterator().asScala.map(v => (v.name(), v)).toMap

}