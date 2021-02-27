package com.louyj.dbsync.sync

import com.louyj.dbsync.sync.ComponentStatus.{ComponentStatus, DEAD, HEALTH, POOR}

import scala.collection.mutable

/**
 * @Author: Louyj
 * @Date: Created at 2021/2/27
 *
 */
class ComponentManager {

  val components: mutable.Map[String, IHeartableComponent] = mutable.Map()

  def addComponent(component: IHeartableComponent) = components += (component.getName -> component)

  def addComponents(components: IHeartableComponent*) = components.foreach(addComponent)

  def addComponents(components: List[IHeartableComponent]) = components.foreach(addComponent)

}


trait IHeartableComponent extends Thread {

  var heartbeatTime: Long = System.currentTimeMillis()

  def heartbeatInterval(): Long

  def heartbeat() = heartbeatTime = System.currentTimeMillis()

  def lastHeartbeatTime() = heartbeatTime

  def componentStatus(): ComponentStatus = {
    val l = (System.currentTimeMillis() - heartbeatTime) / heartbeatInterval()
    if (l < 2) return HEALTH
    if (l < 5) return POOR
    DEAD
  }

}

object ComponentStatus extends Enumeration {

  type ComponentStatus = Value

  val HEALTH = Value("health")
  val POOR = Value("poor")
  val DEAD = Value("dead")

}

