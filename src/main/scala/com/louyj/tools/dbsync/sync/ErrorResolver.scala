package com.louyj.tools.dbsync.sync

import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.DbSyncLanucher.logger
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/25 9:53<br/>
 *
 * @author Louyj<br/>
 */

class ErrorResolver(queueManager: QueueManager, dsPools: DatasourcePools) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  setName("error-resolver")

  override def run(): Unit = {
    while (!isInterrupted) {
      val errorBatch = queueManager.takeError
      logger.warn(s"Receive error batch sync from ${errorBatch.sourceDb} to ${errorBatch.targetDb} size ${errorBatch.ids.size}")

    }
  }

}
