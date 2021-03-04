package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.sync.HeartbeatComponent
import sttp.client.{HttpURLConnectionBackend, UriContext, basicRequest}

class WebhookAlarmSender extends AlarmSender {

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  override def name(): String = "webhook"

  override def sendAlarm(components: Iterable[HeartbeatComponent], syncStatus: SyncState, reason: String,
                         alarmArgs: Map[String, Object]): Unit = {
    implicit val backend = HttpURLConnectionBackend()
    val uri = alarmArgs("uri")
    val content = Map("reason" -> reason, "syncStatus" -> syncStatus, "components" -> components)
    basicRequest.post(uri"$uri")
      .body(jackson.writeValueAsString(content))
      .header("Content-Type", "application/json")
      .send()
  }

}
