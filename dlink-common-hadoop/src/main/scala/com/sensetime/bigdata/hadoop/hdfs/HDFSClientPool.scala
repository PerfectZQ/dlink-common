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
class HDFSClientPool(val factory: HDFSClientFactory,
                     val config: HDFSConfig) extends GenericObjectPool[FileSystem](factory, config) {

  def this(factory: HDFSClientFactory) = {
    this(factory, new HDFSConfig())
  }
}


/**
 * 创建一个单例 HDFSClientPool
 *
 * @author zhangqiang
 * @since 2021/4/26 16:37
 */
object HDFSClientPool {

  @volatile private var defaultPool: HDFSClientPool = _

  /**
   * 初始化连接池
   *
   * @return
   */
  def getDefaultPool(): HDFSClientPool = {
    if (defaultPool == null) {
      HDFSClientPool.synchronized {
        if (defaultPool == null) {
          val config = new HDFSConfig()
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
          val factory = new HDFSClientFactory()
          defaultPool = new HDFSClientPool(factory, config)
          println(s"====> Default HDFSClientPool Created.")
        }
      }
    }
    defaultPool
  }

  /**
   * 程序运行终止时，关闭连接池
   */
  def shutdownDefaultPool(): Unit = {
    if (defaultPool != null) {
      HDFSClientPool.synchronized {
        if (defaultPool != null) {
          defaultPool.close()
          defaultPool = null
          println(s"====> Default HDFSClientPool Closed.")
        }
      }
    }
  }

}


