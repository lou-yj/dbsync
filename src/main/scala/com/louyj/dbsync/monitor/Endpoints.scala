package com.louyj.dbsync.monitor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.DatasourcePools
import com.louyj.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.dbsync.sync.ComponentManager
import io.javalin.Javalin
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/9/18 11:04<br/>
 *
 * @author Louyj<br/>
 */

class Endpoints(val app: Javalin, sysConfig: SysConfig,
                dbConfigs: List[DatabaseConfig],
                dsPools: DatasourcePools,
                componentManager: ComponentManager) {

  val logger = LoggerFactory.getLogger(getClass)
  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  app.get("/status/sync", ctx => {
    val result = dbConfigs.map(dbConfig => {
      val jdbc = dsPools.jdbcTemplate(dbConfig.name)
      val dbOpt = dbOpts(dbConfig.`type`)
      dbOpt.syncState(dbConfig, jdbc)
    })
    ctx.result(jackson.writeValueAsString(result))
  })

  app.get("/status/component", ctx => {
    val status = componentManager.components.map(e => {
      val component = e._2
      component.componentStatus()
      (
        e._1,
        Map(
          "lastHeartbeat" -> new DateTime(component.lastHeartbeatTime()).toString("yyyy-MM-dd HH:mm:ss"),
          "status" -> component.componentStatus().toString
        )
      )
    })
    ctx.result(jackson.writeValueAsString(status))
  })

}

case class SyncState(
                      name: String,
                      pending: Long,
                      blocked: Long,
                      error: Long,
                      success: Long,
                      others: Long
                    )

case class PauseSetting(sourceDb: String,
                        targetDb: String,
                        schema: String,
                        table: String)