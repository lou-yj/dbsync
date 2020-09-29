package com.louyj.tools.dbsync.dbopt

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import com.louyj.tools.dbsync.config.{DatabaseConfig, SyncConfig, SysConfig}
import com.louyj.tools.dbsync.endpoint.SyncState
import com.louyj.tools.dbsync.sync.{SyncData, SyncDataModel, SyncTriggerVersion}
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.{BeanPropertyRowMapper, JdbcTemplate}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 *
 * Create at 2020/8/24 9:43<br/>
 *
 * @author Louyj<br/>
 */

class MysqlOperation extends DbOperation {

  val logger = LoggerFactory.getLogger(getClass)

  override def name(): String = "mysql"

  override def pollBatch(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, batch: Int): List[SyncDataModel] = {
    val sql =
      s"""
      select t1.* from ${dbConfig.sysSchema}.sync_data t1
      left join ${dbConfig.sysSchema}.sync_polled t2
      on t1.id=t2.`dataId`
      where t2.`dataId` is null
      order by t1.id
      limit $batch
    """
    val rowMapper = BeanPropertyRowMapper.newInstance(classOf[SyncDataModel])
    val result = jdbcTemplate.query(sql, rowMapper).asScala.toList
    val args = for (item <- result) yield Array[Object](item.id.asInstanceOf[Object])
    jdbcTemplate.batchUpdate(
      s"""
        insert into ${dbConfig.sysSchema}.sync_polled (`dataId`) values (?)
      """, args.asJava)
    result
  }

  override def prepareBatchUpsert(syncData: SyncData): (String, Array[AnyRef]) = {
    val fieldBuffer = new ListBuffer[String]
    val valueBuffer = new ListBuffer[AnyRef]
    val conflictSetBuffer = new ListBuffer[AnyRef]
    syncData.data.foreach(item => {
      fieldBuffer += s"""`${item._1}`"""
      valueBuffer += item._2
      if (!syncData.keys.contains(item._1)) {
        conflictSetBuffer += s"""`${item._1}` = VALUES(`${item._1}`)"""
      }
    })
    if (conflictSetBuffer.isEmpty) {
      val sql =
        s"""
            insert IGNORE into `${syncData.schema}`.`${syncData.table}`
            (${fieldBuffer.mkString(",")})
            values
            (${(for (_ <- valueBuffer.indices) yield "?").mkString(",")})
          """
      (sql, valueBuffer.toArray)
    } else {
      val sql =
        s"""
            insert into `${syncData.schema}`.`${syncData.table}`
            (${fieldBuffer.mkString(",")})
            values
            (${(for (_ <- valueBuffer.indices) yield "?").mkString(",")})
            ON DUPLICATE KEY UPDATE ${conflictSetBuffer.mkString(",")};
          """
      (sql, valueBuffer.toArray)
    }
  }

  override def prepareBatchDelete(syncData: SyncData): (String, Array[AnyRef]) = {
    val whereBuffer = new ListBuffer[String]
    val whereValueBuffer = new ListBuffer[AnyRef]
    syncData.data.foreach(item => {
      if (syncData.keys.contains(item._1)) {
        whereBuffer += s"""`${item._1}`=?"""
        whereValueBuffer += item._2
      }
    })
    val sql =
      s"""
            delete from `${syncData.schema}`.`${syncData.table}`
            where ${whereBuffer.mkString(" and ")}
          """
    (sql, whereValueBuffer.toArray)
  }

  override def batchAck(jdbc: JdbcTemplate, sysSchema: String, ids: List[Long], status: String, message: String = "") = {
    val ackSql =
      s"""
          insert into $sysSchema.sync_data_status
          (`dataId`,status,message) values (?,?,?)
          ON DUPLICATE KEY UPDATE
          status=values(status),message=values(message),retry=retry+1;
    """
    jdbc.batchUpdate(ackSql, ackArgs(ids, status, message).asJava)
  }


  override def buildInsertTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig): Unit = {
    val dbName = dbConfig.name
    val sysSchema = dbConfig.sysSchema
    val operation = "I"
    val triggerAction = "INSERT"
    val triggerObject = "NEW"
    val triggerName = s"sync_insert_trigger"
    val triggerCondition = if (syncConfig.insertCondition == null) "1=1" else syncConfig.insertCondition
    val searchSql =
      s"""
         select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='${syncConfig.sourceSchema}' and TABLE_NAME='${syncConfig.sourceTable}'
       """
    val objectToJsonArgs = jdbcTemplate.queryForList(searchSql, classOf[String]).asScala.flatMap(column => List(s"'$column'", s"$triggerObject.`$column`")).mkString(",")
    val appendDataSql = syncConfig.targetDb.split(",").map(targetDb =>
      s"""
        insert into $sysSchema.sync_data (`sourceDb`,`targetDb`,`schema`,`table`,`operation`,`data`)
        values
        ('${syncConfig.sourceDb}','$targetDb','${syncConfig.sourceSchema}','${syncConfig.sourceTable}','$operation',JSON_OBJECT($objectToJsonArgs))
      """).mkString(";")
    val sql =
      Array(
        s"DROP TRIGGER IF EXISTS ${syncConfig.sourceSchema}.$triggerName;",
        s"""
        CREATE TRIGGER ${syncConfig.sourceSchema}.$triggerName
        AFTER $triggerAction ON ${syncConfig.sourceSchema}.${syncConfig.sourceTable}
        FOR EACH ROW
        BEGIN
          if $triggerCondition then
            $appendDataSql;
          end if;
        END;
      """)
    val hash = Hashing.murmur3_32().newHasher.putString(sql.mkString(","), StandardCharsets.UTF_8).hash().toString
    if (triggerExists(jdbcTemplate, sysSchema, syncConfig.sourceSchema, syncConfig.sourceTable, triggerName, hash)) {
      logger.debug("Insert trigger for table {}.{}[{}] already exists and matched", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
    } else {
      logger.info("Insert trigger for table {}.{}[{}] not matched, rebuild it", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
      jdbcTemplate.batchUpdate(sql: _*)
      saveTriggerVersion(jdbcTemplate, sysSchema, syncConfig.sourceSchema, syncConfig.sourceTable, triggerName, hash, "")
      logger.info("Insert trigger for table {}.{}[{}] updated", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
    }
  }


  override def buildUpdateTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig): Unit = {
    val dbName = dbConfig.name
    val sysSchema = dbConfig.sysSchema
    val operation = "U"
    val triggerAction = "UPDATE"
    val triggerObject = "NEW"
    val triggerName = s"sync_update_trigger"
    val triggerCondition = if (syncConfig.insertCondition == null) "1=1" else syncConfig.insertCondition
    val searchSql =
      s"""
         select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='${syncConfig.sourceSchema}' and TABLE_NAME='${syncConfig.sourceTable}'
       """
    val objectToJsonArgs = jdbcTemplate.queryForList(searchSql, classOf[String]).asScala.flatMap(column => List(s"'$column'", s"$triggerObject.`$column`")).mkString(",")
    val appendDataSql = syncConfig.targetDb.split(",").map(targetDb =>
      s"""
        insert into $sysSchema.sync_data (`sourceDb`,`targetDb`,`schema`,`table`,`operation`,`data`)
        values
        ('${syncConfig.sourceDb}','$targetDb','${syncConfig.sourceSchema}','${syncConfig.sourceTable}','$operation',JSON_OBJECT($objectToJsonArgs))
      """).mkString(";")
    val sql = Array(
      s"DROP TRIGGER IF EXISTS ${syncConfig.sourceSchema}.$triggerName;",
      s"""
        CREATE TRIGGER ${syncConfig.sourceSchema}.$triggerName
        AFTER $triggerAction ON ${syncConfig.sourceSchema}.${syncConfig.sourceTable}
        FOR EACH ROW
        BEGIN
          if $triggerCondition then
            $appendDataSql;
          end if;
        END;
      """)
    val hash = Hashing.murmur3_32().newHasher.putString(sql.mkString(","), StandardCharsets.UTF_8).hash().toString
    if (triggerExists(jdbcTemplate, sysSchema, syncConfig.sourceSchema, syncConfig.sourceTable, triggerName, hash)) {
      logger.debug("Update trigger for table {}.{}[{}] already exists and matched", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
    } else {
      logger.info("Update trigger for table {}.{}[{}] not matched, rebuild it", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
      jdbcTemplate.batchUpdate(sql: _*)
      saveTriggerVersion(jdbcTemplate, sysSchema, syncConfig.sourceSchema, syncConfig.sourceTable, triggerName, hash, "")
      logger.info("Update trigger for table {}.{}[{}] updated", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
    }
  }

  override def buildDeleteTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate, syncConfig: SyncConfig): Unit = {
    val dbName = dbConfig.name
    val sysSchema = dbConfig.sysSchema
    val operation = "D"
    val triggerAction = "DELETE"
    val triggerObject = "OLD"
    val triggerName = s"sync_delete_trigger"
    val triggerCondition = if (syncConfig.insertCondition == null) "1=1" else syncConfig.insertCondition
    val searchSql =
      s"""
         select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='${syncConfig.sourceSchema}' and TABLE_NAME='${syncConfig.sourceTable}'
       """
    val objectToJsonArgs = jdbcTemplate.queryForList(searchSql, classOf[String]).asScala.flatMap(column => List(s"'$column'", s"$triggerObject.`$column`")).mkString(",")
    val appendDataSql = syncConfig.targetDb.split(",").map(targetDb =>
      s"""
        insert into $sysSchema.sync_data (`sourceDb`,`targetDb`,`schema`,`table`,`operation`,`data`)
        values
        ('${syncConfig.sourceDb}','$targetDb','${syncConfig.sourceSchema}','${syncConfig.sourceTable}','$operation',JSON_OBJECT($objectToJsonArgs))
      """).mkString(";")
    val sql = Array(
      s"DROP TRIGGER IF EXISTS ${syncConfig.sourceSchema}.$triggerName;",
      s"""
        CREATE TRIGGER ${syncConfig.sourceSchema}.$triggerName
        AFTER $triggerAction ON ${syncConfig.sourceSchema}.${syncConfig.sourceTable}
        FOR EACH ROW
        BEGIN
          if $triggerCondition then
            $appendDataSql;
          end if;
        END;
      """)
    val hash = Hashing.murmur3_32().newHasher.putString(sql.mkString(","), StandardCharsets.UTF_8).hash().toString
    if (triggerExists(jdbcTemplate, sysSchema, syncConfig.sourceSchema, syncConfig.sourceTable, triggerName, hash)) {
      logger.debug("Delete trigger for table {}.{}[{}] already exists and matched", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
    } else {
      logger.info("Delete trigger for table {}.{}[{}] not matched, rebuild it", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
      jdbcTemplate.batchUpdate(sql: _*)
      saveTriggerVersion(jdbcTemplate, sysSchema, syncConfig.sourceSchema, syncConfig.sourceTable, triggerName, hash, "")
      logger.info("Delete trigger for table {}.{}[{}] updated", syncConfig.sourceSchema, syncConfig.sourceTable, dbName)
    }
  }

  override def buildSysTable(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate): Unit = {
    val dbName = dbConfig.name
    val schema = dbConfig.sysSchema
    if (schemaExists(jdbcTemplate, schema)) {
      logger.info(s"System schema $schema already exists")
    } else {
      logger.info(s"System schema $schema not exists, rebuild it")
      jdbcTemplate.execute(s"create schema $schema")
      logger.info(s"System schema $schema updated")
    }
    var table = "sync_data"
    if (tableExists(jdbcTemplate, schema, table)) {
      logger.info("System table {}.{}[{}] already exists", schema, table, dbName)
    } else {
      logger.info("System table {}.{}[{}] not exists, rebuild it", schema, table, dbName)
      jdbcTemplate.batchUpdate(
        s"""
          drop table if exists $schema.sync_data CASCADE;
          create table $schema.sync_data
         (
            `id` bigint not null AUTO_INCREMENT PRIMARY KEY,
            `sourceDb` varchar(512),
            `targetDb` varchar(512),
            `schema` varchar(512),
            `table` varchar(512),
            `operation` varchar(10),
            `data` text,
            `createTime` TIMESTAMP not null default CURRENT_TIMESTAMP
         )
        """.split(";"): _*)
      logger.info("System table {}.{}[{}] updated", schema, table, dbName)
    }
    table = "sync_data_status"
    if (tableExists(jdbcTemplate, schema, table)) {
      logger.info("System table {}.{}[{}] already exists", schema, table, dbName)
    } else {
      logger.info("System table {}.{}[{}] not exists, rebuild it", schema, table, dbName)
      jdbcTemplate.batchUpdate(
        s"""
          drop table if exists $schema.sync_data_status CASCADE;
          create table $schema.sync_data_status
         (
            `dataId` bigint REFERENCES $schema.sync_data(id) ON UPDATE CASCADE ON DELETE CASCADE,
            `status` varchar(10),
            `message` text,
            `retry` int default 0,
            `createTime` TIMESTAMP not null default CURRENT_TIMESTAMP,
            index(`dataId`,`status`),
            unique index(`dataId`)
         )
        """.split(";"): _*)
      logger.info("System table {}.{}[{}] updated", schema, table, dbName)
    }
    table = "sync_trigger_version"
    if (tableExists(jdbcTemplate, schema, table)) {
      logger.info("System table {}.{}[{}] already exists", schema, table, dbName)
    } else {
      logger.info("System table {}.{}[{}] not exists, rebuild it", schema, table, dbName)
      jdbcTemplate.batchUpdate(
        s"""
          drop table if exists $schema.sync_trigger_version CASCADE ;
          create table $schema.sync_trigger_version
         (
            `schema` varchar(128),
            `table` varchar(128),
            `trigger` varchar(512),
            `version` varchar(512),
            `function` varchar(512),
            `createTime` TIMESTAMP not null default CURRENT_TIMESTAMP,
            PRIMARY KEY (`schema`,`table`,`trigger`)
         )
        """.split(";"): _*)
      logger.info("System table {}.{}[{}] updated", schema, table, dbName)
    }
    table = "sync_polled"
    if (tableExists(jdbcTemplate, schema, table)) {
      logger.info("System table {}.{}[{}] already exists", schema, table, dbName)
    } else {
      logger.info("System table {}.{}[{}] not exists, rebuild it", schema, table, dbName)
      jdbcTemplate.batchUpdate(
        s"""
          drop table if exists $schema.sync_polled CASCADE ;
          create table $schema.sync_polled
         (
            `dataId` bigint REFERENCES $schema.sync_data(id) ON UPDATE CASCADE ON DELETE CASCADE,
            `createTime` TIMESTAMP not null default CURRENT_TIMESTAMP,
            PRIMARY KEY (`dataId`)
         )
        """.split(";"): _*)
      logger.info("System table {}.{}[{}] updated", schema, table, dbName)
    }
  }

  override def cleanSysTable(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, keepHours: Int) = {
    val sql =
      s"""
        delete from ${dbConfig.sysSchema}.sync_data where id in
        (select `dataId` from ${dbConfig.sysSchema}.sync_data_status where status='OK'
        and `createTime` < TIMESTAMPADD(HOUR,${-keepHours}, CURRENT_TIMESTAMP)
        );
      """
    val count = jdbcTemplate.update(sql)
    val vacuumSql =
      s"""
        OPTIMIZE TABLE ${dbConfig.sysSchema}.sync_data;
        OPTIMIZE TABLE ${dbConfig.sysSchema}.sync_data_status;
        OPTIMIZE TABLE ${dbConfig.sysSchema}.sync_trigger_version;
        OPTIMIZE TABLE ${dbConfig.sysSchema}.sync_polled
      """
    jdbcTemplate.batchUpdate(vacuumSql.split(";"): _*)
    count
  }

  override def buildBootstrapState(jdbcTemplate: JdbcTemplate, dbConfig: DatabaseConfig, sysConfig: SysConfig) = {
    val sql =
      s"""
       delete from ${dbConfig.sysSchema}.sync_data_status where status='BLK'
       or (status='ERR' and retry < ${sysConfig.maxRetry});
     """
    val count = jdbcTemplate.update(sql)
    val pooledSql =
      s"""
        delete from ${dbConfig.sysSchema}.sync_polled;
        insert into ${dbConfig.sysSchema}.sync_polled(`dataId`)
        select `dataId` from ${dbConfig.sysSchema}.sync_data_status;
        OPTIMIZE TABLE ${dbConfig.sysSchema}.sync_polled
      """
    jdbcTemplate.batchUpdate(pooledSql.split(";"): _*)
    count
  }

  def triggerExists(jdbcTemplate: JdbcTemplate,
                    sysSchema: String,
                    schema: String, table: String, trigger: String, version: String) = {
    val sql =
      s"""
        select count(1) from INFORMATION_SCHEMA.TRIGGERS tg
        left join $sysSchema.sync_trigger_version tv
        on tg.EVENT_OBJECT_SCHEMA=tv.`schema` and tg.`EVENT_OBJECT_TABLE`=tv.`table` and tv.`trigger`=tg.TRIGGER_NAME
        where tv.`schema`=? and tv.`table`=? and tv.`trigger`=? and tv.`version`=?
    """
    val num = jdbcTemplate.queryForObject(sql, Array[AnyRef](schema, table, trigger, version), classOf[Long])
    num > 0
  }

  def saveTriggerVersion(jdbcTemplate: JdbcTemplate, sysSchema: String,
                         schema: String, table: String, trigger: String,
                         version: String, function: String) = {
    val sql =
      s"""
        insert into $sysSchema.sync_trigger_version
        (`schema`,`table`,`trigger`,`version`,`function`)
        values
        (?,?,?,?,?)
        ON DUPLICATE KEY UPDATE
        `version`=values(`version`),`function`=values(`function`)
    """
    jdbcTemplate.update(sql, Array[AnyRef](schema, table, trigger, version, function): _*)
    ()
  }


  override def tableExists(jdbcTemplate: JdbcTemplate,
                           schema: String, table: String) = {
    val sql =
      s"""
        select count(1) from INFORMATION_SCHEMA.tables where TABLE_SCHEMA =? and TABLE_NAME =?
    """
    val num = jdbcTemplate.queryForObject(sql, Array[AnyRef](schema, table), classOf[Long])
    num > 0
  }

  def schemaExists(jdbcTemplate: JdbcTemplate,
                   schema: String) = {
    val sql =
      s"""
         select count(1) from INFORMATION_SCHEMA.schemata where SCHEMA_NAME =?
       """

    val num = jdbcTemplate.queryForObject(sql, Array[AnyRef](schema), classOf[Long])
    num > 0
  }

  override def uniqueIndexExists(jdbcTemplate: JdbcTemplate,
                                 schema: String, table: String, indexColumns: String) = {
    val sql =
      s"""
    select count(1) from
    (
    select GROUP_CONCAT(COLUMN_NAME) columns
    from INFORMATION_SCHEMA.STATISTICS
    where NON_UNIQUE=0 and TABLE_SCHEMA=? and TABLE_NAME=?
    order by SEQ_IN_INDEX
    ) t
    where columns=?
    """
    val num = jdbcTemplate.queryForObject(sql, Array[AnyRef](schema, table, indexColumns), classOf[Long])
    num > 0
  }

  override def listTriggers(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate): List[SyncTriggerVersion] = {
    val sql =
      s"""
           select * from ${dbConfig.sysSchema}.sync_trigger_version
         """
    val rowMapper = BeanPropertyRowMapper.newInstance(classOf[SyncTriggerVersion])
    jdbcTemplate.query(sql, rowMapper).asScala.toList
  }

  override def deleteTrigger(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate,
                             schema: String, table: String, trigger: String, function: String): Unit = {
    val sql =
      s"""
          drop trigger if exists $schema.$trigger;
        """
    try {
      jdbcTemplate.update(sql)
    } catch {
      case e: Exception => logger.warn(s"Drop trigger $trigger for table $schema.$table failed, reason ${e.getClass.getName}-${e.getMessage}")
    }
    val delSql =
      s"""
         delete from ${dbConfig.sysSchema}.sync_trigger_version
          where `schema`=? and `table`=? and `trigger`=?;
       """
    jdbcTemplate.update(delSql, Array[Object](schema, table, trigger))
  }

  override def syncState(dbConfig: DatabaseConfig,
                         jdbcTemplate: JdbcTemplate): SyncState = {
    val pendingSql =
      s"""
         select count(1) from ${dbConfig.sysSchema}.sync_data d
         left join ${dbConfig.sysSchema}.sync_data_status s
         on d.id =s.`dataId` where s.`dataId` is null
       """
    val pending = jdbcTemplate.queryForObject(pendingSql, classOf[Long])
    val blockedSql =
      s"""
         select count(1) from ${dbConfig.sysSchema}.sync_data d
         left join ${dbConfig.sysSchema}.sync_data_status s
         on d.id =s.`dataId` where s.`status` ='BLK'
       """
    val blocked = jdbcTemplate.queryForObject(blockedSql, classOf[Long])
    val errorSql =
      s"""
         select count(1) from ${dbConfig.sysSchema}.sync_data d
         left join ${dbConfig.sysSchema}.sync_data_status s
         on d.id =s.`dataId` where s.`status` ='ERR'
       """
    val error = jdbcTemplate.queryForObject(errorSql, classOf[Long])
    val successSql =
      s"""
         select count(1) from ${dbConfig.sysSchema}.sync_data d
         left join ${dbConfig.sysSchema}.sync_data_status s
         on d.id =s.`dataId` where s.`status` ='OK'
       """
    val success = jdbcTemplate.queryForObject(successSql, classOf[Long])
    val othersSql =
      s"""
         select count(1) from ${dbConfig.sysSchema}.sync_data d
         left join ${dbConfig.sysSchema}.sync_data_status s
         on d.id =s.`dataId` where s.`status` not in ('OK','ERR','BLK')
       """
    val others = jdbcTemplate.queryForObject(othersSql, classOf[Long])
    SyncState(dbConfig.name, pending, blocked, error, success, others)
  }

  def createUniqueIndex(jdbcTemplate: JdbcTemplate,
                        schema: String, table: String, indexColumns: String): Unit = {
    val sql =
      s"""
     create unique index idx_${System.currentTimeMillis()} on $schema.$table($indexColumns)
   """
    jdbcTemplate.execute(sql)
  }

  def ackArgs(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield Array[AnyRef](id.asInstanceOf[AnyRef], status, message)
  }

}
