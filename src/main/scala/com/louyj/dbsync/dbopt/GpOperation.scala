package com.louyj.dbsync.dbopt

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.sync.SyncData
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.SqlValue

import java.sql.{JDBCType, PreparedStatement, SQLException}
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 *
 * Create at 2020/8/24 9:43<br/>
 *
 * @author Louyj<br/>
 */

class GpOperation extends PgOperation {

  private val jackson = new ObjectMapper()
  jackson.registerModule(DefaultScalaModule)

  val dollar = "$$"

  override def name(): String = "greenplum"

  override def prepareBatchUpsert(syncData: SyncData): (String, Array[AnyRef]) = {
    val sql = "select public.gp_upsert(?,?,?,?)"
    val keys = for (key <- syncData.keys) yield key.asInstanceOf[AnyRef]
    (sql, Array[AnyRef](syncData.schema, syncData.table, ArraySqlValue.create(keys), jackson.writeValueAsString(syncData.data)))
  }

  override def batchAck(jdbc: JdbcTemplate, sysSchema: String, ids: List[Long], status: String, message: String): Array[Int] = {
    val ackSql = s"select public.gp_upsert($sysSchema,'sync_data_status',?,?)"
    jdbc.batchUpdate(ackSql, ackArgsGp(ids, status, message).asJava)
  }

  override def saveTriggerVersion(jdbcTemplate: JdbcTemplate, sysSchema: String, schema: String, table: String,
                                  trigger: String, version: String, function: String): Unit = {
    val sql = s"select public.gp_upsert($sysSchema,'sync_trigger_version',?,?)"
    val map = Map("schema" -> schema, "table" -> table, "trigger" -> trigger, "version" -> version, "function" -> function)
    jdbcTemplate.update(sql, Array[AnyRef](ArraySqlValue.create(Array[AnyRef]("schema", "table", "trigger"), jackson.writeValueAsString(map))): _*)
    ()
  }


  override def buildSysTable(dbConfig: DatabaseConfig, jdbcTemplate: JdbcTemplate): Unit = {
    super.buildSysTable(dbConfig, jdbcTemplate)
    val dbName = dbConfig.name
    val function = "gp_upsert"
    val dollar = "$$"
    val sql =
      s"""
        create or replace function public.$function(nsp name, tbl name, keys text[], icontent text, out rcnt int) returns int as $dollar
      declare
        k text;
        v text;
        sql1 text := 'update '||quote_ident(nsp)||'.'||quote_ident(tbl)||' set ';
        sql2 text := 'insert into '||quote_ident(nsp)||'.'||quote_ident(tbl)||' (';
        sql3 text := 'values (';
        sql4 text := ' where ';
      begin
        rcnt := 0;
        for k,v in select * from json_each_text(icontent::json)
        loop
          if (not array[k] && keys) then
            sql1 := sql1||quote_ident(k)||'='||coalesce(quote_literal(v),'NULL')||',';
          else
            sql4 := sql4||quote_ident(k)||'='||coalesce(quote_literal(v),'NULL')||' and ';
          end if;
        end loop;

        execute rtrim(sql1, ',') || rtrim(sql4, 'and ');
        GET DIAGNOSTICS rcnt = ROW_COUNT;
        if rcnt=0 then

          for k,v in select * from json_each_text(icontent::json)
          loop
            sql2 := sql2||quote_ident(k)||',';
            sql3 := sql3||coalesce(quote_literal(v),'NULL')||',';
          end loop;

          execute rtrim(sql2, ',') || ') ' || rtrim(sql3, ',') || ') ';
        end if;

        return;

        exception when others then
          execute rtrim(sql1, ',') || rtrim(sql4, 'and ');
          return;
      end;
      $dollar language plpgsql strict;
      """
    jdbcTemplate.execute(sql)
    logger.info(s"System function public.$function[$dbName] updated")
  }

  def ackArgsGp(ids: List[Long], status: String, message: String) = {
    for (id <- ids) yield {
      val map = Map("dataId" -> id, "status" -> status, "message" -> message)
      Array[AnyRef](ArraySqlValue.create(Array[AnyRef]("dataId")), jackson.writeValueAsString(map))
    }
  }


  object ArraySqlValue {
    def create(arr: Array[AnyRef]) = new ArraySqlValue(arr, JDBCType.VARCHAR.name().toLowerCase())

    def create(arr: Array[AnyRef], dbTypeName: String) = new ArraySqlValue(arr, dbTypeName)

  }

  class ArraySqlValue private(val arr: Array[AnyRef], val dbTypeName: String) extends SqlValue {

    @throws[SQLException]
    override def setValue(ps: PreparedStatement, paramIndex: Int): Unit = {
      val arrayValue = ps.getConnection.createArrayOf(dbTypeName, arr)
      ps.setArray(paramIndex, arrayValue)
    }

    override def cleanup(): Unit = {
    }
  }

}
