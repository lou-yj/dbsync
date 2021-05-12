package com.louyj.dbsync.component

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

  def format(components: mutable.Map[String, HeartbeatComponent]) = {
    components.map(e => {
      val component = e._2
      val props: mutable.Map[String, Any] = mutable.Map(
        "lastHeartbeat" -> new DateTime(component.lastHeartbeatTime()).toString("yyyy-MM-dd HH:mm:ss"),
        "status" -> component.componentStatus().toString,
        "heartbeatInterval" -> component.heartbeatInterval(),
        "heartbeatLost" -> component.heartbeatLost()
      )
      component match {
        case com: StatisticsComponent =>
          props += ("statistics" -> com.statistics, "total" -> com.totalCount)
        case _ =>
      }
      (
        e._1,
        props
      )
    })
  }

}






