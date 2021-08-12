package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem.getDefaultUri
import org.apache.hadoop.util.StringUtils

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter

/**
 * FileSystem Connection Pool based on Apache common-pool2
 *
 * @author zhangqiang
 * @since 2021/4/26 14:30
 */
class FileSystemObjectPool(val factory: FileSystemPooledObjectFactory,
                           val config: FileSystemObjectPoolConfig) extends GenericObjectPool[FileSystem](factory, config) {

  def this(factory: FileSystemPooledObjectFactory) = {
    this(factory, new FileSystemObjectPoolConfig())
  }

}


/**
 * 创建一个单例 HDFSClientPool
 *
 * @author zhangqiang
 * @since 2021/4/26 16:37
 */
object FileSystemObjectPool {

  class Key(conf: Configuration) {

    private val uri = getDefaultUri(conf)
    private val scheme: String = if (uri.getScheme == null) "" else StringUtils.toLowerCase(uri.getScheme)
    private val authority: String = if (uri.getAuthority == null) "" else StringUtils.toLowerCase(uri.getAuthority)

    override def hashCode: Int = (scheme + authority).hashCode

    private def isEqual(a: Any, b: Any) = (a == b) || (a != null && a.equals(b))

    override def equals(obj: Any): Boolean = {
      if (this == obj) return true
      if (obj != null && obj.isInstanceOf[Key]) {
        val that = obj.asInstanceOf[Key]
        return isEqual(this.scheme, that.scheme) && isEqual(this.authority, that.authority)
      }
      false
    }

    override def toString: String = scheme + "://" + authority
  }

  @volatile private val poolCache: ConcurrentHashMap[Key, FileSystemObjectPool] = new ConcurrentHashMap[Key, FileSystemObjectPool]()

  /**
   * 初始化连接池
   *
   * @return
   */
  def getDefaultPool(factory: FileSystemPooledObjectFactory, config: FileSystemObjectPoolConfig): FileSystemObjectPool = {
    val key = new Key(factory.configuration)
    poolCache.computeIfAbsent(key, (_: Key) => new FileSystemObjectPool(factory, config))
  }

  /**
   *
   * @param pool
   */
  def shutdownPool(pool: FileSystemObjectPool): Unit = {
    if (pool != null) {
      pool.synchronized {
        if (pool != null) {
          pool.close()
          poolCache.remove(new Key(pool.factory.configuration))
          println(s"====> HDFSClientPool $pool Closed.")
        }
      }
    }
  }

  /**
   * 程序运行终止时，关闭连接池
   */
  def shutdownDefaultPool(): Unit = {
    poolCache.elements().asScala.foreach(shutdownPool)
    poolCache.clear()
  }

}


