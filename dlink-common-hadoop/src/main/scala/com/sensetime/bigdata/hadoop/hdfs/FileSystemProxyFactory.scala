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
          // 不知道为什么 target 从对象池中拿出来后 getConf 返回 null，导致调用 FileSystem.open(Path) 出现 NPE
          target.setConf(FileSystemProxyFactory.configuration)
          proxy.setConf(FileSystemProxyFactory.configuration)
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
  val configuration = new Configuration()
  private val factory = new FileSystemPooledObjectFactory(configuration)
  private val config: FileSystemObjectPoolConfig = FileSystemObjectPool.getDefaultHDFSConfig
  private lazy val pool: FileSystemObjectPool = FileSystemObjectPool.getDefaultPool(factory, config)

}
