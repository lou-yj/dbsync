package com.louyj.tools.dbsync.sync

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.leansoft.bigqueue.{BigQueueImpl, IBigQueue}
import com.louyj.tools.dbsync.DatasourcePools
import com.louyj.tools.dbsync.config.{DatabaseConfig, SysConfig}
import com.louyj.tools.dbsync.dbopt.DbOperationRegister.dbOpts
import org.apache.commons.io.FileUtils
import org.mapdb.{BTreeMap, DBMaker}
import org.slf4j.LoggerFactory

/**
 *
 * Create at 2020/8/25 13:45<br/>
 *
 * @author Louyj<br/>
 */

class StateManger(sysConfig: SysConfig, dbconfigs: List[DatabaseConfig], dsPools: DatasourcePools) {

  val jackson = new ObjectMapper()
  jackson.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
  jackson.registerModule(DefaultScalaModule)
  val logger = LoggerFactory.getLogger(getClass)

  val stateDb = buildDb
  val blockedMap = buildBlockedMap
  val retryMap = buildRetryMap
  val blockedQueue: IBigQueue = buildBlockedQueue
  val retryQueue: IBigQueue = buildRetryQueue

  dbconfigs.foreach(cleanBlockedStatus)


  implicit def string2ByteArray(str: String) = str.getBytes(UTF_8)

  implicit def byteArray2String(bytes: Array[Byte]) = new String(bytes, UTF_8)

  def isBlocked(hash: Long) = retryMap.containsKey(hash)

  def blockedIds(hash: Long) = retryMap.getOrDefault(hash, Set[Long]())

  def updateBlocked(hash: Long, ids: Set[Long]): Unit = if (ids.isEmpty) retryMap.remove(hash) else retryMap.put(hash, ids)

  def removeBlocked(hash: Long) = blockedMap.remove(hash)

  def block(blockedData: BlockedData) = {
    val hash = blockedData.data.items.head.hash
    val list = blockedMap.getOrDefault(hash, List())
    blockedMap.put(hash, list :+ blockedData)
    val json = jackson.writeValueAsString(blockedData)
    blockedQueue.enqueue(json)
  }

  def errorRetry(data: ErrorBatch) = {
    for (pair <- data.hashs zip data.ids) {
      val ids = blockedIds(pair._1)
      val nids = if (ids.isEmpty) Set(pair._2) else ids + pair._2
      retryMap.put(pair._1, nids)
    }
    retryQueue.enqueue(jackson.writeValueAsString(data))
  }

  def takeError = {
    val bytes = retryQueue.dequeue()
    if (bytes == null) null else jackson.readValue(bytes, classOf[ErrorBatch])
  }

  def takeBlocked = {
    val bytes = blockedQueue.dequeue()
    if (bytes == null) null else jackson.readValue(bytes, classOf[BlockedData])
  }

  def cleanBlockedStatus(dbConfig: DatabaseConfig) = {
    val dbOpt = dbOpts(dbConfig.`type`)
    val jdbc = dsPools.jdbcTemplate(dbConfig.name)
    val num = dbOpt.cleanBlockedStatus(jdbc, dbConfig, sysConfig)
    logger.info(s"Cleaned $num data in blocked status for ${dbConfig.name}")
  }

  def buildDb = {
    val stateDir = new File(sysConfig.workDirectory, sysConfig.stateDirectory)
    val blockedFile: File = new File(stateDir, "state.map")
    logger.info(s"Using file ${blockedFile.getAbsolutePath} to save state")
    if (blockedFile.exists()) blockedFile.delete()
    blockedFile.getParentFile.mkdirs()
    DBMaker.fileDB(blockedFile.getAbsolutePath).fileMmapEnableIfSupported.fileMmapPreclearDisable.make()
  }

  def buildBlockedMap = {
    stateDb.treeMap("blocked").createOrOpen().asInstanceOf[BTreeMap[Long, List[BlockedData]]]
  }

  def buildRetryMap = {
    stateDb.treeMap("retry").createOrOpen().asInstanceOf[BTreeMap[Long, Set[Long]]]
  }

  def buildBlockedQueue = {
    val stateDir = new File(sysConfig.workDirectory, sysConfig.stateDirectory)
    val queueDir = new File(stateDir, "blocked")
    FileUtils.deleteDirectory(queueDir)
    logger.info(s"Using directory ${queueDir.getAbsolutePath} to serve as blocked queue")
    new BigQueueImpl(queueDir.getAbsolutePath, "blocked")
  }

  def buildRetryQueue = {
    val stateDir = new File(sysConfig.workDirectory, sysConfig.stateDirectory)
    val queueDir = new File(stateDir, "retry")
    FileUtils.deleteDirectory(queueDir)
    logger.info(s"Using directory ${queueDir.getAbsolutePath} to serve as retry queue")
    new BigQueueImpl(queueDir.getAbsolutePath, "retry")
  }

}



