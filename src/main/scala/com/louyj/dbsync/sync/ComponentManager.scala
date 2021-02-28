package com.louyj.dbsync.sync

import com.louyj.dbsync.sync.ComponentStatus.{ComponentStatus, GREEN, RED, YELLOW}
import org.joda.time.DateTime

import scala.collection.mutable

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/27
 *
 */
class ComponentManager {

  val components: mutable.Map[String, HeartbeatComponent] = mutable.Map()

  def addComponent(component: HeartbeatComponent) = components += (component.getName -> component)

  def addComponents(components: HeartbeatComponent*) = components.foreach(addComponent)

  def addComponents(components: List[HeartbeatComponent]) = components.foreach(addComponent)

}


trait HeartbeatComponent extends Thread {

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

