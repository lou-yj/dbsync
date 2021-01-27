package com.louyj.dbsync.dbopt

import com.google.common.hash.Hashing
import com.louyj.dbsync.config.{DatabaseConfig, SyncConfig, SysConfig}
import com.louyj.dbsync.endpoint.SyncState
import com.louyj.dbsync.sync.{SyncData, SyncDataModel, SyncTriggerVersion}
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.{BeanPropertyRowMapper, JdbcTemplate}

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/24 9:43<br/>
 *
 * @author Louyj<br/>
 */

class GpOperation extends PgOperation {

  override def name(): String = "greenplum"

  override def prepareBatchUpsert(syncData: SyncData): (String, Array[AnyRef]) = {
    super.prepareBatchUpsert(syncData)
  }

}
