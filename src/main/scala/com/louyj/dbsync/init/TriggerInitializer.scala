package com.louyj.dbsync.init

import com.louyj.dbsync.DatasourcePools
import com.louyj.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.dbsync.job.TriggerSync
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
  cleanTrigger(dsPools, srcDbConfig, syncConfigs)


  def buildTrigger(syncConfig: SyncConfig) = {
    syncConfig.targetDb.split(",").foreach(targetdb => {
      val tarDbConfig = dbconfigs(targetdb)
      syncTrigger(srcDbConfig, tarDbConfig, dsPools, syncConfig)
    })
  }

}
