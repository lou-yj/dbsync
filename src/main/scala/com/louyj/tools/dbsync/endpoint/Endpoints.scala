package com.louyj.tools.dbsync.endpoint

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import io.javalin.Javalin
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/9/18 11:04<br/>
 *
 * @author Louyj<br/>
 */

class Endpoints(sysConfig: SysConfig, dbConfigs: List[DatabaseConfig], dsPools: DatasourcePools) {

  val logger = LoggerFactory.getLogger(getClass)
  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  logger.info(s"Endpoints listen on ${sysConfig.endpointPort}")
  val app = Javalin.create(config => {
    config.showJavalinBanner = false
  }).start(sysConfig.endpointPort)

  app.get("/sync-status", ctx => {
    val result = dbConfigs.map(dbConfig => {
      val jdbc = dsPools.jdbcTemplate(dbConfig.name)
      val dbOpt = dbOpts(dbConfig.`type`)
      dbOpt.syncState(dbConfig, jdbc)
    })
    ctx.result(jackson.writeValueAsString(result))
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