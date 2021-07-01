package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.fs.FileSystem

/**
 * @author zhangqiang
 * @since 2021/4/26 14:33
 */
class HDFSConfig extends GenericObjectPoolConfig[FileSystem] {

  // 在空闲时检查有效性, 默认false
  this.setTestWhileIdle(true)
  // 逐出连接的最小空闲时间
  this.setMinEvictableIdleTimeMillis(60000L)
  // 逐出扫描的时间间隔(毫秒) 如果为负数则不运行逐出线程，默认-1
  this.setTimeBetweenEvictionRunsMillis(-1)
  // 每次逐出检查时, 逐出的最大数目
  this.setNumTestsPerEvictionRun(-1)

}
