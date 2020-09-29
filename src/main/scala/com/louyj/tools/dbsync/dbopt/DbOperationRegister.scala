package com.louyj.tools.dbsync.dbopt

/**
 *
 * Create at 2020/8/27 17:12<br/>
 *
 * @author Louyj<br/>
 */

object DbOperationRegister {

  val pgOpt = new PgOperation
  val mysqlOpt = new MysqlOperation

  val dbOpts = Map(pgOpt.name() -> pgOpt,
    mysqlOpt.name() -> mysqlOpt)

}