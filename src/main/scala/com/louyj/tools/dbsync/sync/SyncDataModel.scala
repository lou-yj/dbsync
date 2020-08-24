package com.louyj.tools.dbsync.sync

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


case class SyncData(id: Long, operation: String,
                    schema: String, table: String, key: Array[String]
                    , data: Map[String, AnyRef])

case class BatchData(targetDb: String, partition: Int, items: ListBuffer[SyncData])

case class AckData(dbName: String, ids: List[Long], status: String, message: String)