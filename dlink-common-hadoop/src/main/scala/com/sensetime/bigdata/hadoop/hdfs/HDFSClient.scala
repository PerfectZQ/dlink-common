package com.sensetime.bigdata.hadoop.hdfs

import org.apache.hadoop.fs.FileSystem

import java.io.Closeable


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
          fileSystem = HDFSClientPool.getDefaultPool().borrowObject()
        }
      }
    }
    fileSystem
  }

  override def close(): Unit = {
    if (fileSystem != null) {
      this.synchronized {
        if (fileSystem != null) {
          HDFSClientPool.getDefaultPool().returnObject(fileSystem)
          fileSystem = null
        }
      }
    }
  }

}