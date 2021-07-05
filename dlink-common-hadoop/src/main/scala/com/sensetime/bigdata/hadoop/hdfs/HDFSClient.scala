package com.sensetime.bigdata.hadoop.hdfs

import com.sensetime.bigdata.hadoop.bean.KerberosConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import java.io.Closeable


/**
 * HDFSClient
 * <p>
 * TODO: FileSystem 代理类
 *
 * @author zhangqiang
 * @since 2021/4/26 16:37
 */
class HDFSClient(configuration: Configuration = new Configuration()) extends Closeable {

  @volatile private var fileSystem: FileSystem = _

  private val factory = new HDFSClientFactory(configuration)

  def open(): FileSystem = {
    if (fileSystem == null) {
      this.synchronized {
        if (fileSystem == null) {
          fileSystem = HDFSClientPool.getDefaultPool(factory, HDFSClientPool.getDefaultHDFSConfig).borrowObject()
        }
      }
    }
    fileSystem
  }

  override def close(): Unit = {
    if (fileSystem != null) {
      this.synchronized {
        if (fileSystem != null) {
          HDFSClientPool.getDefaultPool(factory, HDFSClientPool.getDefaultHDFSConfig).returnObject(fileSystem)
          fileSystem = null
        }
      }
    }
  }

}