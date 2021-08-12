package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.fs.FileSystem

import java.net.URL
import java.util.Properties

/**
 * @author zhangqiang
 * @since 2021/4/26 14:33
 */
class FileSystemObjectPoolConfig extends GenericObjectPoolConfig[FileSystem] {

  this.setTestOnBorrow(true)
  // 在空闲时检查有效性, 默认 false
  this.setTestWhileIdle(false)
  // 逐出扫描的时间间隔(毫秒) 如果为负数则不运行逐出线程，默认-1
  this.setTimeBetweenEvictionRunsMillis(30000L)
  // 逐出连接的最小空闲时间
  this.setMinEvictableIdleTimeMillis(60000L)
  // 每次逐出检查时, 逐出的最大数目
  this.setNumTestsPerEvictionRun(-1)

}

object FileSystemObjectPoolConfig {

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

}
