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

@BeanProperty
class SyncDataModel {

  var id: Long = _
  var sourceDb: String = _
  var targetDb: String = _
  var schema: String = _
  var table: String = _
  var operation: String = _
  var data: String = _
  var createTime: Timestamp = _
}


case class SyncData(id: Long, operation: String,
                    schema: String, table: String, key: Array[String]
                    , data: Map[String, AnyRef])

case class BatchData(targetDb: String, partition: Int, items: ListBuffer[SyncData])

case class AckData(dbName: String, ids: List[Long], status: String, message: String)