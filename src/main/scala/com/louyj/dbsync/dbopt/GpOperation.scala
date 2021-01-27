package com.louyj.dbsync.dbopt

import com.louyj.dbsync.sync.SyncData
import org.springframework.jdbc.core.JdbcTemplate

import scala.collection.JavaConverters.seqAsJavaListConverter
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
    val setBuffer = new ListBuffer[String]
    val setValueBuffer = new ListBuffer[AnyRef]
    val whereBuffer = new ListBuffer[String]
    val whereValueBuffer = new ListBuffer[AnyRef]
    val fieldBuffer = new ListBuffer[String]
    val valueBuffer = new ListBuffer[AnyRef]
    syncData.data.foreach(item => {
      fieldBuffer += s"""\"${item._1}\""""
      valueBuffer += item._2
      if (!syncData.keys.contains(item._1)) {
        setBuffer += s"""\"${item._1}\" = ?"""
        setValueBuffer += item._2
      } else {
        whereBuffer += s"""\"${item._1}\" = ?"""
        whereValueBuffer += item._2
      }
    })
    val sql =
      s"""
         do $$
        begin
           update \"${syncData.schema}\".\"${syncData.table}\" set ${setBuffer.mkString(",")} where ${whereBuffer.mkString(" and ")} ;
           if not found then
              insert into \"${syncData.schema}\".\"${syncData.table}\" (${fieldBuffer.mkString(",")}) values (${(for (_ <- fieldBuffer.indices) yield "?").mkString(",")});
           end if;
        end $$;
       """
    (sql, (setValueBuffer ++ whereValueBuffer ++ valueBuffer).toArray)
  }

  override def batchAck(jdbc: JdbcTemplate, sysSchema: String, ids: List[Long], status: String, message: String): Array[Int] = {
    val ackSql =
      s"""
         do $$
        begin
           update $sysSchema.sync_data_status set status=?,message=?,retry=retry+1 where "dataId"=? ;
           if not found then
              insert into $sysSchema.sync_data_status
              ("dataId",status,message) values (?,?,?);
           end if;
        end $$;
       """
    jdbc.batchUpdate(ackSql, ackArgsGp(ids, status, message).asJava)
  }

  override def saveTriggerVersion(jdbcTemplate: JdbcTemplate, sysSchema: String, schema: String, table: String,
                                  trigger: String, version: String, function: String): Unit = {
    val sql =
      s"""
         do $$
        begin
           update test set "version"=?,"function"=? where "schema"=? and "table"=? and "trigger"=?;
           if not found then
               insert into $sysSchema.sync_trigger_version
              ("schema","table","trigger","version","function")
              values
              (?,?,?,?,?);
           end if;
        end $$;
       """
    jdbcTemplate.update(sql, Array[AnyRef](version, function, schema, table, trigger, schema, table, trigger, version, function): _*)
    ()
  }

  def ackArgsGp(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield Array[AnyRef](status, message, id.asInstanceOf[AnyRef], id.asInstanceOf[AnyRef], status, message)
  }

}
