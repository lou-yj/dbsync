package com.louyj.tools.dbsync.sync

import java.util.concurrent.TimeUnit

import com.louyj.tools.dbsync.DatasourcePools
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/25 11:13<br/>
 *
 * @author Louyj<br/>
 */

class BlockedHandler(queueManager: QueueManager, dsPools: DatasourcePools) extends Thread {

  val logger = LoggerFactory.getLogger(getClass)

  setName("blocked-handler")

  override def run(): Unit = {
    while (!isInterrupted) {
      try {
        val tuple = queueManager.takeBlocked()

      } catch {
        case e: InterruptedException => throw e
        case e: Exception => {
          logger.error("Blocked handler failed", e)
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }
  }

}
