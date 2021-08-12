package com.sensetime.bigdata.hadoop.hdfs

import net.sf.cglib.proxy.{Enhancer, MethodInterceptor, MethodProxy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation

import java.lang.reflect.Method
import java.security.PrivilegedAction

/**
 * Proxy class of [[FileSystem]].
 *
 * @author zhangqiang
 * @since 2021/7/9 14:02
 * @param factory
 * @param config
 * @param maxWaitMillis Max wait millis of borrowing object from an object pool
 */
class FileSystemProxyFactory(factory: FileSystemPooledObjectFactory,
                             config: FileSystemObjectPoolConfig,
                             maxWaitMillis: Long) extends MethodInterceptor {

  private val objectPool: FileSystemObjectPool = FileSystemObjectPool.getDefaultPool(factory, config)

  private val target: FileSystem = objectPool.borrowObject(maxWaitMillis)

  private val configuration: Configuration = target.getConf

  @volatile private var proxy: FileSystem = _

  val ugi: UserGroupInformation = objectPool.factory.ugi

  def this(factory: FileSystemPooledObjectFactory) {
    this(factory, FileSystemObjectPoolConfig.getDefaultHDFSConfig, -1)
  }

  def this() {
    this(new FileSystemPooledObjectFactory(new Configuration()))
  }


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
    ugi.doAs(new PrivilegedAction[Object] {
      override def run(): Object = {
        val returnValue = method.getName match {
          case "close" =>
            objectPool.returnObject(target)
            Unit
          case _ => method.invoke(target, args: _*)
        }
        returnValue
      }
    })
  }

}

