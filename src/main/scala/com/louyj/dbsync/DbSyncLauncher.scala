package com.louyj.dbsync

import scala.util.control.Breaks.{break, breakable}

/**
 *
 * Create at 2020/8/23 15:11<br/>
 *
 * @author Louyj<br/>
 */

object DbSyncLauncher {

  var restartFlag = false
  var restartReason = "N/A"

  def restart(reason: String) = {
    restartFlag = true
    restartReason = reason
  }

  def main(args: Array[String]): Unit = {
    breakable {
      while (true) {
        restartFlag = false
        App.bootstrap(args, restartReason)
        if (restartFlag == false) {
          break
        }
      }
    }
  }

}

object DbSyncLanucher {

  def main(args: Array[String]): Unit = {
    DbSyncLauncher.main(args)
  }

}
