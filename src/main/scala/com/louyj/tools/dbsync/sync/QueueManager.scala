package com.louyj.tools.dbsync.sync

import java.util.concurrent.ArrayBlockingQueue

/**
 *
 * Create at 2020/8/23 18:54<br/>
 *
 * @author Louyj<br/>
 */

class QueueManager(val partition: Int) {

  val queues = (for (i <- 0 until partition) yield i -> new ArrayBlockingQueue[BatchData](2)).toMap


  def queue(partition: Int) = queues.get(partition)

  def put(partition: Int, data: BatchData) = queue(partition).get.put(data)

  def take(partition: Int) = queue(partition).get.take()


}
