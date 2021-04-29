package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{PooledObject, PooledObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.IOException


/**
 * HDFS Client Factory: 管理连接对象的创建，销毁，验证等动作
 *
 * @author zhangqiang
 * @since 2021/4/26 15:47
 */
class HDFSClientFactory(configuration: Configuration = new Configuration())
  extends PooledObjectFactory[FileSystem] {

  /**
   * 创建一个池化的对象
   *
   * @return
   */
  override def makeObject(): PooledObject[FileSystem] = {
    val fileSystem = FileSystem.newInstance(configuration)
    new DefaultPooledObject[FileSystem](fileSystem)
  }

  /**
   * 销毁一个池化的对象
   *
   * @return
   */
  override def destroyObject(pooledObject: PooledObject[FileSystem]): Unit = {
    val fileSystem: FileSystem = pooledObject.getObject
    fileSystem.close()
  }

  /**
   * 校验池化对象是否可用
   *
   * @param pooledObject
   * @return
   */
  override def validateObject(pooledObject: PooledObject[FileSystem]): Boolean = {
    val fileSystem: FileSystem = pooledObject.getObject
    try {
      fileSystem.exists(new Path("/"))
      println(s"====> validateObject true")
      true
    } catch {
      case _: IOException =>
        println(s"====> validateObject false")
        false
    }
  }

  override def activateObject(pooledObject: PooledObject[FileSystem]): Unit = {

  }

  override def passivateObject(pooledObject: PooledObject[FileSystem]): Unit = {

  }

}
