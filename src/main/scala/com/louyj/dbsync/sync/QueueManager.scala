package com.louyj.dbsync.sync

import com.louyj.dbsync.SystemContext
import org.slf4j.LoggerFactory

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 18:54<br/>
 *
 * @author Louyj<br/>
 */

class QueueManager(state: StateManger, ctx: SystemContext) {

  val logger = LoggerFactory.getLogger(getClass)

  val queues = (for (i <- 0 until ctx.sysConfig.partition) yield i -> new ArrayBlockingQueue[BatchData](10)).toMap

  val unBlockedEventQueues = (for (i <- 0 until ctx.sysConfig.partition) yield i -> new ArrayBlockingQueue[Long](100)).toMap


  def queue(partition: Int) = queues(partition)

  def put(partition: Int, data: BatchData) = queue(partition).put(data)

  def take(partition: Int): BatchData = {
    queue(partition).synchronized {
      val batchData = queue(partition).poll(ctx.sysConfig.pollBlockInterval, TimeUnit.MILLISECONDS)
      if (batchData == null) return null
      var blocked = false
      batchData.items.foreach(d => if (state.isBlocked(d.hash)) blocked = true)
      if (blocked) {
        val blockedDatas = new ListBuffer[BlockedData]
        val unblockedDatas = new ListBuffer[SyncData]
        batchData.items.foreach(d => {
          val ids = state.blockedIds(d.hash)
          if (ids.isEmpty) {
            unblockedDatas += d
          } else {
            val bData = BatchData(batchData.sourceDb, batchData.targetDb, batchData.partition, ListBuffer(d))
            val blockedData = BlockedData(ids, bData)
            blockedDatas += blockedData
          }
        })
        batchData.items = unblockedDatas
        blockedDatas.foreach(state.block)
      }
      batchData
    }
  }

  def putError(partition: Int, errorBatch: ErrorBatch) = {
    queue(partition).synchronized {
      state.errorRetry(errorBatch)
    }
  }

  def takeError: ErrorBatch = {
    while (true) {
      val errorBatch = state.takeError
      if (errorBatch != null) return errorBatch
      TimeUnit.SECONDS.sleep(1)
    }
    null
  }

  def resolvedError(partition: Int, hash: Long, id: Long) = {
    queue(partition).synchronized {
      val ids = state.blockedIds(hash)
      val nids = ids - id
      state.updateBlocked(hash, nids)
      if (nids.isEmpty) {
        unBlockedEventQueues(partition).put(hash)
        logger.info(s"Hash slot $hash blocked was resolved, wakeup data consumer")
      } else {
        logger.info(s"Hash slot $hash still blocked by $ids, throught $id is marked resolved")
      }
    }
  }

  def takeBlocked(): BlockedData = {
    while (true) {
      val errorBatch = state.takeBlocked
      if (errorBatch != null) return errorBatch
      TimeUnit.SECONDS.sleep(1)
    }
    null
  }

  def pollBlocked(partition: Int) = {
    val hash = unBlockedEventQueues(partition).poll()
    if (hash == 0) List() else {
      state.removeBlocked(hash)
    }

  }


}
