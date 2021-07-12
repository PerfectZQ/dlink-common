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

  private val configuration: Configuration = target.getConf

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
          // 调用 en.create() 的时候，由于使用的是无参构造方法，会执行 FileSystem 的父类 Configured 的无参构造函数，
          // 从而执行 setConf(null)，导致 target 中的 conf 被覆盖为 null，当执行 FileSystem.open(Path) 会调用
          // getConf().getInt("io.file.buffer.size", 4096) 从而出现 NPE，这里重新设置回去
          proxy.setConf(configuration)
          println(s"====> Create proxy instance $proxy of target $target, conf=${target.getConf}")
        }
      }
    }
    proxy
  }

  @throws(classOf[Throwable])
  override def intercept(obj: Object, method: Method, args: Array[Object], proxy: MethodProxy): Object = {
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
