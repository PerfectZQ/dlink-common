package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.hadoop.fs.FileSystem

import java.net.URL
import java.util.Properties

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

  @volatile private var defaultPool: FileSystemObjectPool = _

  /**
   * 初始化连接池
   *
   * @return
   */
  def getDefaultPool(factory: FileSystemPooledObjectFactory, config: FileSystemObjectPoolConfig): FileSystemObjectPool = {
    if (defaultPool == null) {
      FileSystemObjectPool.synchronized {
        if (defaultPool == null) {
          defaultPool = new FileSystemObjectPool(factory, config)
          println(s"====> Default HDFSClientPool Created.")
        }
      }
    }
    defaultPool
  }

  def getDefaultHDFSConfig: FileSystemObjectPoolConfig = {
    val config = new FileSystemObjectPoolConfig()
    val urls: java.util.Enumeration[URL] = this.getClass.getClassLoader.getResources("hdfs.properties")
    val properties: Properties = new Properties()
    while (urls.hasMoreElements) {
      val url: URL = urls.nextElement()
      properties.load(url.openStream())
    }
    config.setMaxIdle(properties.getProperty("hdfs.pool.maxIdle", "8").toInt)
    config.setMaxTotal(properties.getProperty("hdfs.pool.maxTotal", "8").toInt)
    config.setMinIdle(properties.getProperty("hdfs.pool.minIdle", "0").toInt)
    config.setMaxWaitMillis(properties.getProperty("hdfs.pool.maxWaitMillis", "-1").toLong)
    config
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
          println(s"====> HDFSClientPool $pool Closed.")
        }
      }
    }
  }

  /**
   * 程序运行终止时，关闭连接池
   */
  def shutdownDefaultPool(): Unit = {
    shutdownPool(defaultPool)
    defaultPool = null
  }

}


