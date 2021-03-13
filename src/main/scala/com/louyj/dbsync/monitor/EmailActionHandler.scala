package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class EmailActionHandler extends ActionHandler {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  override def name(): String = "email"

  override def doAction(monitorConfig: MonitorConfig, ctx: SystemContext,
                        components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String): Unit = {
    val content =
      s"""
          matched rule: ${monitorConfig.name}
          reason: $reason
          sync status: ${jackson.writeValueAsString(syncStatus)}
          components status: ${jackson.writeValueAsString(components)}
       """
    val params = monitorConfig.params;
    val email = new SimpleEmail
    email.setHostName(params("host").asInstanceOf[String])
    email.setSmtpPort(params("port").asInstanceOf[Int])
    email.setAuthenticator(new DefaultAuthenticator(params("user").asInstanceOf[String], params("password").asInstanceOf[String]))
    email.setSSLOnConnect(true)
    email.setFrom(params("from").asInstanceOf[String])
    email.setSubject(params("subject").asInstanceOf[String])
    email.setMsg(content)
    params("to").asInstanceOf[List[String]].foreach(email.addTo(_))
    email.send();
  }

}
