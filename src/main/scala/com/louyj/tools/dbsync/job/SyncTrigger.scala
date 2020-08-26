package com.louyj.tools.dbsync.job

import java.util.TimerTask

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig}
import com.louyj.tools.dbsync.init.funcs.syncTrigger
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/24 18:06<br/>
 *
 * @author Louyj<br/>
 */

class SyncTrigger(dsPools: DatasourcePools, dbConfigs: List[DatabaseConfig], syncConfigs: List[SyncConfig])
  extends TimerTask {

  val logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    for (dbconfig <- dbConfigs;
         syncConfig <- syncConfigs if syncConfig.sourceDb == dbconfig.name) {
      syncTrigger(dbconfig, dsPools, syncConfig)
    }
  }

}
