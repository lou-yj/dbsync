package com.louyj.dbsync.sync

import java.sql.Timestamp

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/23 18:02<br/>
 *
 * @author Louyj<br/>
 */


class SyncDataModel {

  @BeanProperty var id: Long = _
  @BeanProperty var sourceDb: String = _
  @BeanProperty var targetDb: String = _
  @BeanProperty var schema: String = _
  @BeanProperty var table: String = _
  @BeanProperty var operation: String = _
  @BeanProperty var data: String = _
  @BeanProperty var createTime: Timestamp = _
}


case class SyncData(hash: Long, id: Long, operation: String,
                    schema: String, table: String, keys: Array[String]
                    , data: Map[String, AnyRef])

case class BatchData(sourceDb: String, targetDb: String, partition: Int, var items: ListBuffer[SyncData])

case class AckData(dbName: String, ids: List[Long], status: String, message: String)

case class ErrorBatch(sourceDb: String, targetDb: String, schema: String, table: String, sql: String, args: List[Array[AnyRef]],
                      ids: List[Long], hashs: List[Long], reason: String)

@SerialVersionUID(1L)
case class BlockedData(blockedBy: Set[Long], data: BatchData)


class SyncTriggerVersion {
  @BeanProperty var schema: String = _
  @BeanProperty var table: String = _
  @BeanProperty var trigger: String = _
  @BeanProperty var function: String = _
}