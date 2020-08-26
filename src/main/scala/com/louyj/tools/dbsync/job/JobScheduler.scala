package com.louyj.tools.dbsync.job

import java.util.Timer

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig, SysConfig}
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/26 12:27<br/>
 *
 * @author Louyj<br/>
 */

class JobScheduler(dsPools: DatasourcePools,
                   sysConfig: SysConfig, dbConfigs: List[DatabaseConfig],
                   syncConfigs: List[SyncConfig]) {

  val logger = LoggerFactory.getLogger(getClass)

  val timer = new Timer("cronjob", true);

  val cleanWorker = new CleanWorker(dsPools, sysConfig, dbConfigs)
  timer.schedule(cleanWorker, sysConfig.cleanInterval, sysConfig.cleanInterval)
  logger.info(s"Clean worker lanuched, scheduled at fixed rate of ${sysConfig.cleanInterval}ms")

  val syncTrigger = new SyncTrigger(dsPools, dbConfigs, syncConfigs)
  timer.schedule(syncTrigger, sysConfig.syncTriggerInterval, sysConfig.syncTriggerInterval)
  logger.info(s"Sync trigger worker lanuched, scheduled at fixed rate of ${sysConfig.syncTriggerInterval}ms")


}
