package com.sensetime.bigdata.hadoop.hdfs

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{PooledObject, PooledObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.DelegationTokenRenewer.Renewable
import org.apache.hadoop.fs.{DelegationTokenRenewer, FileSystem, Path}

/**
 * HDFS Client Factory: 管理连接对象的创建，销毁，验证等动作
 *
 * @author zhangqiang
 * @since 2021/4/26 15:47
 */
class FileSystemPooledObjectFactory(configuration: Configuration = new Configuration())
  extends PooledObjectFactory[FileSystem] {

  /**
   * 创建一个池化的对象
   *
   * @return
   */
  override def makeObject(): PooledObject[FileSystem] = {
    val fileSystem = FileSystem.newInstance(configuration)
    // val fileSystem = FileSystem.get(ConfigurationUtil.disableCache(configuration))
    println(s"====> HDFSClientFactory makeObject: $fileSystem，conf=${fileSystem.getConf}")
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
    println(s"====> HDFSClientFactory destroyObject: $fileSystem")
  }

  /**
   * 校验池化的对象是否可用
   *
   * @param pooledObject
   * @return
   */
  override def validateObject(pooledObject: PooledObject[FileSystem]): Boolean = {
    val fileSystem: FileSystem = pooledObject.getObject
    try {
      fileSystem.exists(new Path("/"))
      println(s"====> HDFSClientFactory validateObject: $fileSystem true.")
      true
    } catch {
      case e: Exception =>
        println(s"====> HDFSClientFactory validateObject: $fileSystem false, caused by $e")
        false
    }
  }

  /**
   * 重新初始化从对象池返回的对象
   * <p>
   * 重新刷新 Kerberos 认证
   *
   * @param pooledObject
   */
  override def activateObject(pooledObject: PooledObject[FileSystem]): Unit = {
    val fileSystem: FileSystem = pooledObject.getObject
    fileSystem match {
      case renewable: Renewable =>
        DelegationTokenRenewer.getInstance().addRenewAction(renewable)
      case _ =>
        println(s"====> HDFSClientFactory activateObject: FileSystem $fileSystem is not Renewable.")
    }
    /*
    val renewer = "sre.bigdata@HADOOP.DATA.SENSETIME.COM"
    val token = fileSystem.getDelegationToken(renewer)
    var expire = 0L
    if (token != null) {
      expire = token.renew(configuration)
    }
    println(s"====> HDFSClientFactory activateObject: Renew DelegationToken of $renewer, token=$token, expire=$expire")
    */
  }

  override def passivateObject(pooledObject: PooledObject[FileSystem]): Unit = {

  }

}
