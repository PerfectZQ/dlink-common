package com.sensetime.bigdata.hadoop.hdfs

import net.sf.cglib.proxy.{Enhancer, MethodInterceptor, MethodProxy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem

import java.lang.reflect.Method

/**
 * Proxy class of [[FileSystem]].
 *
 * @author zhangqiang
 * @since 2021/7/9 14:02
 * @param maxWaitMillis Max wait millis of borrowing object from an object pool
 */
class FileSystemProxyFactory(maxWaitMillis: Long = -1) extends MethodInterceptor {

  private val target: FileSystem = FileSystemProxyFactory.pool.borrowObject(maxWaitMillis)

  @volatile private var proxy: FileSystem = _

  /**
   * Get a proxy object for target object
   * <p>
   * Note: The none arg constructor of FileSystem's super class [[org.apache.hadoop.conf.Configured]]
   * will set conf null default. It'll cause NPE when invoke [[FileSystem.open(Path)]]
   *
   * @return
   */
  def getProxyInstance: FileSystem = {
    if (proxy == null) {
      this.synchronized {
        if (proxy == null) {
          val en = new Enhancer
          // Set supper class of proxy object
          en.setSuperclass(target.getClass)
          en.setCallback(this)
          // Create proxy object: ...$$EnhancerByCGLIB$$5b0d50e0
          proxy = en.create().asInstanceOf[FileSystem]
          proxy.setConf(target.getConf)
          println(s"====> Create proxy instance $proxy of target $target, conf=${target.getConf}")
        }
      }
    }
    proxy
  }

  @throws(classOf[Throwable])
  override def intercept(obj: Object, method: Method, args: Array[Object], proxy: MethodProxy): Object = {
    println(s"====> Proxy intercept: method=${method.getName}, args=${args.mkString(",")}")
    val returnValue = method.getName match {
      case "close" =>
        FileSystemProxyFactory.pool.returnObject(target)
        Unit
      case _ => method.invoke(target, args: _*)
    }
    returnValue
  }

}

object FileSystemProxyFactory {

  private val factory = new FileSystemPooledObjectFactory(new Configuration())
  private val config: FileSystemObjectPoolConfig = FileSystemObjectPool.getDefaultHDFSConfig
  private lazy val pool: FileSystemObjectPool = FileSystemObjectPool.getDefaultPool(factory, config)

}
