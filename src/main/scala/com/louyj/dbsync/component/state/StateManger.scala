package com.louyj.dbsync.component.state

import com.leansoft.bigqueue.{BigQueueImpl, IBigQueue}
import com.louyj.dbsync.SystemContext
import com.louyj.dbsync.config.DatabaseConfig
import com.louyj.dbsync.dbopt.DbOperationRegister.dbOpts
import com.louyj.dbsync.sync.{BlockedData, ErrorBatch}
import org.apache.commons.io.FileUtils
import org.mapdb.{BTreeMap, DBMaker}
import org.slf4j.LoggerFactory

import java.io._

/**
 *
 * Create at 2020/8/25 13:45<br/>
 *
 * @author Louyj<br/>
 */

class StateManger(ctx: SystemContext) {

  val logger = LoggerFactory.getLogger(getClass)

  val stateDb = buildDb
  val blockedMap = buildBlockedMap
  val retryMap = buildRetryMap
  val blockedQueue: IBigQueue = buildBlockedQueue
  val retryQueue: IBigQueue = buildRetryQueue

  ctx.stateManger = this
  ctx.dbConfigs.foreach(cleanBlockedStatus)


  def isBlocked(hash: Long) = retryMap.containsKey(hash)

  def blockedIds(hash: Long) = retryMap.getOrDefault(hash, Set[Long]())

  def updateBlocked(hash: Long, ids: Set[Long]): Unit = if (ids.isEmpty) retryMap.remove(hash) else retryMap.put(hash, ids)

  def removeBlocked(hash: Long): List[BlockedData] = blockedMap.remove(hash)


  private def serialize(any: Any) = {
    val baas = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baas)
    oos.writeObject(any)
    oos.close()
    baas.toByteArray
  }

  private def deserialize(bytes: Array[Byte]) = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val any = ois.readObject()
    ois.close()
    any
  }

  def block(blockedData: BlockedData) = {
    val hash = blockedData.data.items.head.hash
    val list = blockedMap.getOrDefault(hash, List())
    blockedMap.put(hash, list :+ blockedData)
    blockedQueue.enqueue(serialize(blockedData))
  }

  def errorRetry(data: ErrorBatch) = {
    for (pair <- data.hashs zip data.ids) {
      val ids = blockedIds(pair._1)
      val nids = if (ids.isEmpty) Set(pair._2) else ids + pair._2
      retryMap.put(pair._1, nids)
    }
    retryQueue.enqueue(serialize(data))
  }

  def takeError = {
    val bytes = retryQueue.dequeue()
    if (bytes == null) null else deserialize(bytes).asInstanceOf[ErrorBatch]
  }

  def takeBlocked = {
    val bytes = blockedQueue.dequeue()
    if (bytes == null) null else deserialize(bytes).asInstanceOf[BlockedData]
  }

  def cleanBlockedStatus(dbConfig: DatabaseConfig) = {
    val dbOpt = dbOpts(dbConfig.`type`)
    val jdbc = ctx.dsPools.jdbcTemplate(dbConfig.name)
    val num = dbOpt.buildBootstrapState(jdbc, dbConfig, ctx.sysConfig)
    logger.info(s"Cleaned $num data in blocked status for ${dbConfig.name}")
  }

  def buildDb = {
    val stateDir = new File(ctx.sysConfig.workDirectory, ctx.sysConfig.stateDirectory)
    val blockedFile: File = new File(stateDir, "state.map")
    logger.info(s"Using file ${blockedFile.getAbsolutePath} to save state")
    if (blockedFile.exists()) blockedFile.delete()
    blockedFile.getParentFile.mkdirs()
    DBMaker.fileDB(blockedFile.getAbsolutePath)
      .fileMmapEnableIfSupported
      .fileMmapPreclearDisable.make()
  }

  def buildBlockedMap = {
    stateDb.treeMap("blocked").createOrOpen().asInstanceOf[BTreeMap[Long, List[BlockedData]]]
  }

  def buildRetryMap = {
    stateDb.treeMap("retry").createOrOpen().asInstanceOf[BTreeMap[Long, Set[Long]]]
  }

  def buildBlockedQueue = {
    val stateDir = new File(ctx.sysConfig.workDirectory, ctx.sysConfig.stateDirectory)
    val queueDir = new File(stateDir, "blocked")
    FileUtils.deleteDirectory(queueDir)
    logger.info(s"Using directory ${queueDir.getAbsolutePath} to serve as blocked queue")
    new BigQueueImpl(queueDir.getAbsolutePath, "blocked")
  }

  def buildRetryQueue = {
    val stateDir = new File(ctx.sysConfig.workDirectory, ctx.sysConfig.stateDirectory)
    val queueDir = new File(stateDir, "retry")
    FileUtils.deleteDirectory(queueDir)
    logger.info(s"Using directory ${queueDir.getAbsolutePath} to serve as retry queue")
    new BigQueueImpl(queueDir.getAbsolutePath, "retry")
  }

}



