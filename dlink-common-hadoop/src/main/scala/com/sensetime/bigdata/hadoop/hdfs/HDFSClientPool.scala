package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.hadoop.fs.FileSystem

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



