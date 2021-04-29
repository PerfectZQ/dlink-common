package com.sensetime.bigdata.hadoop.hdfs

import org.apache.hadoop.fs.FileSystem

import java.io.Closeable
import java.net.URL
import java.util.Properties


/**
 * 创建一个单例 HDFSClientPool
 *
 * @author zhangqiang
 * @since 2021/4/26 16:37
 */
object HDFSClient {

  private lazy val pool: HDFSClientPool = initPool()

  /**
   * 初始化连接池
   *
   * @return
   */
  private def initPool(): HDFSClientPool = {
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
    new HDFSClientPool(factory, config)
  }

  /**
   * 程序运行终止时，关闭连接池
   */
  def shutdown(): Unit = {
    if (pool != null)
      pool.close()
  }

}

/**
 * HDFSClient
 *
 * @author zhangqiang
 * @since 2021/4/26 16:37
 */
class HDFSClient extends Closeable {

  @volatile private var fileSystem: FileSystem = _

  def open(): FileSystem = {
    if (fileSystem == null) {
      this.synchronized {
        if (fileSystem == null) {
          fileSystem = HDFSClient.pool.borrowObject()
        }
      }
    }
    fileSystem
  }

  override def close(): Unit = {
    if (fileSystem != null) {
      this.synchronized {
        if (fileSystem != null) {
          HDFSClient.pool.returnObject(fileSystem)
          fileSystem = null
        }
      }
    }
  }

}