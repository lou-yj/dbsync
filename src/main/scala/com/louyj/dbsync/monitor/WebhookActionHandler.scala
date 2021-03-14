package com.louyj.dbsync.monitor

import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.MonitorConfig
import com.louyj.dbsync.util.JsonUtils
import org.slf4j.LoggerFactory
import sttp.client.{HttpURLConnectionBackend, UriContext, basicRequest}

import scala.collection.mutable

class WebhookActionHandler extends ActionHandler {

  val logger = LoggerFactory.getLogger(getClass)

  private val jackson = JsonUtils.jackson()

  override def name(): String = "webhook"

  override def doAction(monitorConfig: MonitorConfig, ctx: SystemContext, components: mutable.Map[String, mutable.Map[String, Any]],
                        syncStatus: SyncState, reason: String): Unit = {
    implicit val backend = HttpURLConnectionBackend()
    val webhookParams = jackson.convertValue(monitorConfig.params, classOf[WebhookParams])
    val content = Map("matchedRule" -> monitorConfig.name, "reason" -> reason,
      "syncStatus" -> syncStatus, "components" -> components)
    val response = basicRequest.post(uri"${webhookParams.url}")
      .body(jackson.writeValueAsString(content))
      .header("Content-Type", "application/json")
      .send()
    logger.info(s"Url ${webhookParams.url} response code ${response.code} content ${response.body}")
  }

}

case class WebhookParams(url: String)

