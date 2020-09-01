package com.louyj.tools.dbsync.init

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.job.TriggerSync
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/23 16:12<br/>
 *
 * @author Louyj<br/>
 */


class TriggerInitializer(srcDbConfig: DatabaseConfig, dbconfigs: Map[String, DatabaseConfig],
                         dsPools: DatasourcePools,
                         syncConfigs: List[SyncConfig]) extends TriggerSync {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info(s"Start check trigger status for ${srcDbConfig.name}")
  syncConfigs.filter(_.sourceDb == srcDbConfig.name).foreach(buildTrigger)
  logger.info(s"Finish check trigger status for ${srcDbConfig.name}")


  def buildTrigger(syncConfig: SyncConfig) = {
    syncConfig.targetDb.split(",").foreach(targetdb => {
      val tarDbConfig = dbconfigs(targetdb)
      syncTrigger(srcDbConfig, tarDbConfig, dsPools, syncConfig)
    })
  }

}
