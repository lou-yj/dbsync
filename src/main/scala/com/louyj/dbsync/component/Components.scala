package com.louyj.dbsync.component

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.component.ComponentStatus.{ComponentStatus, GREEN, RED, YELLOW}
import io.javalin.Javalin
import org.joda.time.DateTime

import scala.collection.mutable

/**
 * @Author: Louyj
 * @Date: Created at 2021/5/12
 *
 */

trait Component {

  var ctx: SystemContext

  def name(): String = this.getClass.getSimpleName

  def order(): Int = 0

  def open(ctx: SystemContext): Unit = {
    this.ctx = ctx
  }

  def close(): Unit = {}

}

trait ComponentConfigAware {

  var config: Map[String, Any]

  def withConfig(config: Map[String, Any]): Unit = {
    this.config = config
  }

}

trait ScheduleComponent extends Component with Runnable {

  def rate(): Long

  def delay(): Long

}

trait EndpointsComponent extends Component {

  var app: Javalin

  def withJavalin(app: Javalin): Unit = {
    this.app = app
  }

}

trait HeartbeatComponent extends Component {

  var heartbeatTime: Long = System.currentTimeMillis()

  def heartbeatInterval(): Long

  def heartbeat() = heartbeatTime = System.currentTimeMillis()

  def lastHeartbeatTime() = heartbeatTime

  def componentStatus(): ComponentStatus = {
    val l = (System.currentTimeMillis() - heartbeatTime) / heartbeatInterval()
    if (l < 2) return GREEN
    if (l < 5) return YELLOW
    RED
  }

  def heartbeatLost() = (System.currentTimeMillis() - heartbeatTime) / heartbeatInterval()

}

trait StatisticsComponent extends HeartbeatComponent {

  var totalCount = 0L
  var statistics: mutable.Map[String, Long] = mutable.Map()
  var statisticsKeys: mutable.Set[String] = mutable.Set()

  def incr(num: Long) = {
    totalCount = totalCount + num
    val day = DateTime.now().toString(statisticsTimeFormat())
    val count = statistics.getOrElse(day, 0L)
    statistics += (day -> (count + num))
    statisticsKeys += day
    if (statisticsKeys.size > statisticsKeepCount()) {
      statisticsKeys.toList.sorted
        .take(statisticsKeys.size - statisticsKeepCount)
        .foreach(statistics -= _)
    }
  }

  def statisticsTimeFormat(): String

  def statisticsKeepCount(): Int = 7

}

trait DayStatisticsComponent extends StatisticsComponent {

  override def statisticsTimeFormat() = "yyyy-MM-dd"

  override def statisticsKeepCount(): Int = 7

}

trait HourStatisticsComponent extends StatisticsComponent {
  override def statisticsTimeFormat() = "yyyy-MM-dd HH"

  override def statisticsKeepCount(): Int = 7 * 24

}

object ComponentStatus extends Enumeration {

  type ComponentStatus = Value

  val GREEN = Value("GREEN")
  val YELLOW = Value("YELLOW")
  val RED = Value("RED")

}