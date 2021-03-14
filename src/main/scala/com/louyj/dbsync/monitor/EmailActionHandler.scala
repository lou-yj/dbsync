package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import com.louyj.dbsync.util.JsonUtils
import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class EmailActionHandler extends ActionHandler {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = JsonUtils.jackson()
  private val yaml = JsonUtils.yaml()

  override def name(): String = "email"

  override def doAction(monitorConfig: MonitorConfig, ctx: SystemContext,
                        components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String): Unit = {
    var content =
      s"""
matched rule: ${monitorConfig.name}
reason: $reason"""
    if (syncStatus != null) {
      content = content +
        s"""
sync status:
${appendBlank(yaml.writeValueAsString(syncStatus), "  ")}"""
    }
    if (components != null) {
      content = content +
        s"""
components status:
${appendBlank(yaml.writeValueAsString(components), "  ")}"""
    }
    val params = monitorConfig.params
    val emailParams = jackson.convertValue(params, classOf[EmailParams])
    val email = new SimpleEmail
    email.setHostName(emailParams.host)
    email.setSmtpPort(emailParams.port)
    email.setAuthenticator(new DefaultAuthenticator(emailParams.user, emailParams.password))
    email.setSSLOnConnect(true)
    email.setFrom(emailParams.from)
    email.setSubject(emailParams.subject)
    email.setMsg(content)
    email.addTo(emailParams.to.toArray: _*)
    email.send();
  }

  def appendBlank(content: String, blank: String) = {
    content.split("\n").map(blank + _).mkString("\n")
  }

}

case class EmailParams(
                        host: String,
                        port: Int,
                        user: String,
                        password: String,
                        from: String,
                        subject: String,
                        to: List[String]
                      )