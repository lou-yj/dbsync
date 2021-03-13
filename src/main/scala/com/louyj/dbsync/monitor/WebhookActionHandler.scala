package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.SystemContext
import org.slf4j.LoggerFactory
import sttp.client.{HttpURLConnectionBackend, UriContext, basicRequest}

import scala.collection.mutable

class WebhookActionHandler extends ActionHandler {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  override def name(): String = "webhook"

  override def doAction(ctx: SystemContext, components: mutable.Map[String, mutable.Map[String, Any]], syncStatus: SyncState, reason: String,
                        alarmArgs: Map[String, Object]): Unit = {
    implicit val backend = HttpURLConnectionBackend()
    val uri = alarmArgs("url")
    val content = Map("reason" -> reason, "syncStatus" -> syncStatus, "components" -> components)
    basicRequest.post(uri"$uri")
      .body(jackson.writeValueAsString(content))
      .header("Content-Type", "application/json")
      .send()
    logger.warn(s"Webhook action triggered, reason $reason")
  }

}