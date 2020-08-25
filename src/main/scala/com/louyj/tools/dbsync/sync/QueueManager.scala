package com.louyj.tools.dbsync.sync

import java.util.concurrent.ArrayBlockingQueue

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 18:54<br/>
 *
 * @author Louyj<br/>
 */

class QueueManager(val partition: Int) {

  val logger = LoggerFactory.getLogger(getClass)

  val queues = (for (i <- 0 until partition) yield i -> new ArrayBlockingQueue[BatchData](2)).toMap

  val unBlockedEventQueues = (for (i <- 0 until partition) yield i -> new ArrayBlockingQueue[Long](100)).toMap

  val errorQueue = new ArrayBlockingQueue[ErrorBatch](100)

  val blockedQueue = new ArrayBlockingQueue[BlockedData](100)

  val blockedHashs = new mutable.HashMap[Long, mutable.Set[Long]]


  def queue(partition: Int) = queues.get(partition)

  def put(partition: Int, data: BatchData) = queue(partition).get.put(data)

  def take(partition: Int) = this.synchronized {
    val batchData = queue(partition).get.take()
    var blocked = false
    batchData.items.foreach(d => if (blockedHashs.contains(d.hash)) blocked = true)
    if (blocked) {
      val blockedDatas = new ListBuffer[BlockedData]
      val unblockedDatas = new ListBuffer[SyncData]
      batchData.items.foreach(d => {
        val id = blockedHashs.get(d.hash)
        if (id.isEmpty || id.get.isEmpty) {
          unblockedDatas += d
        } else {
          val blockedData = BlockedData(batchData.sourceDb, d.hash, id.get.toSet)
          blockedDatas += blockedData
        }
      })
      batchData.items = unblockedDatas
      blockedDatas.foreach(blockedQueue.put)
    }
    batchData
  }

  def putError(errorBatch: ErrorBatch) = this.synchronized {
    for (pair <- errorBatch.hashs zip errorBatch.ids) {
      val ids = blockedHashs.get(pair._1)
      if (ids.isEmpty) {
        blockedHashs(pair._1) = mutable.Set(pair._2)
      } else {
        ids.get += pair._2
      }
    }
    errorQueue.put(errorBatch)
  }

  def takeError = errorQueue.take()

  def resolvedError(hash: Long, id: Long) = this.synchronized {
    val ids = blockedHashs(hash)
    ids -= id
    if (ids.isEmpty) {
      blockedHashs -= hash
      val partition = math.abs(hash % this.partition).intValue
      unBlockedEventQueues(partition).put(hash)
      blockedHashs.remove(hash)
    } else {
      logger.info(s"Hash slot $hash still blocked by $ids, throught $id is marked resolved")
    }
  }

  def takeBlocked() = blockedQueue.take()


}
