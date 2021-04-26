package com.sensetime.bigdata.hadoop.hdfs

import com.sensetime.bigdata.hadoop.kerberos.KerberosAuthentication

import java.io.File
import java.security.PrivilegedAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation

import org.slf4j.LoggerFactory


/**
 * @author zhangqiang
 * @since 2021/4/12 14:32
 */
object HDFSHelper {
  val internalUsername = "sre.bigdata"
  private val defaultCluster = "hadoop"
  private val systemSep = System.getProperty("file.separator")
  val classPath: String = this.getClass.getClassLoader.getResource("").getPath
}

/**
 * 支持创建跨集群 Kerberos 认证的 FileSystem
 *
 * @param clusterNameToConnect     要连接的 Hadoop 集群名称
 * @param hadoopConfPathToConnect  要连接 Hadoop 集群的配置文件加载路径
 * @param username                 Kerberos 认证的 principal
 * @param keytabPath               Kerberos 认证的 keytab 路径
 * @param clusterNameOfKerberos    要认证的 KDC 集群名称
 * @param hadoopConfPathOfKerberos 要认证的 KDC 集群的配置文件路径
 * @param krb5confPath             Kerberos 认证的 krb5.conf 路径，主要用于拼接 Realm 和查找 KDC Server 地址
 * @param trustStorePath           Hadoop trustStore 文件路径
 * @param kerberosEnable           是否开启 Kerberos 认证
 * @param debug                    是否开启 Kerberos 认证的 debug 模式
 * @param useSubjectCredsOnly
 */
class HDFSHelper(private var clusterNameToConnect: String,
                 private var hadoopConfPathToConnect: String,
                 private var username: String,
                 private var keytabPath: String,
                 private var clusterNameOfKerberos: String,
                 private var hadoopConfPathOfKerberos: String,
                 private var krb5confPath: String,
                 private var trustStorePath: String,
                 private var kerberosEnable: Boolean = true,
                 private var debug: Boolean = false,
                 private var useSubjectCredsOnly: Boolean = false) extends Serializable {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val defaultKrb5ConfPath = s"${HDFSHelper.classPath}hadoop-clusters/$clusterNameToConnect/kerberos/krb5.conf"
  private lazy val defaultHadoopConf = s"${HDFSHelper.classPath}hadoop-clusters/$clusterNameToConnect/conf"
  private lazy val defaultTrustStorePath = s"${HDFSHelper.classPath}hadoop-clusters/$clusterNameToConnect/kerberos/$clusterNameToConnect.truststore"

  private var ugi: UserGroupInformation = _

  logger.info(
    s"""===> HDFSHelper:
       |{
       |  clusterNameToConnect: $clusterNameToConnect,
       |  hadoopConfPathToConnect: $hadoopConfPathToConnect,
       |  username: $username,
       |  keytabPath: $keytabPath,
       |  clusterNameOfKerberos: $clusterNameOfKerberos,
       |  hadoopConfPathOfKerberos: $hadoopConfPathOfKerberos,
       |  krb5confPath: $krb5confPath,
       |  trustStorePath: $trustStorePath,
       |  kerberosEnable: $kerberosEnable,
       |  debug: $debug,
       |  useSubjectCredsOnly: $useSubjectCredsOnly
       |}
       |""".stripMargin)

  val fileSystem: FileSystem = defaultFileSystem()

  def this(clusterNameToConnect: String,
           username: String,
           keytabPath: String,
           clusterNameOfKerberos: String,
           applicationHome: String) {
    this(
      clusterNameToConnect,
      hadoopConfPathToConnect = Array(applicationHome, "hadoop-clusters", clusterNameToConnect, "conf").mkString(HDFSHelper.systemSep),
      username,
      keytabPath,
      clusterNameOfKerberos,
      krb5confPath = Array(applicationHome, "hadoop-clusters", clusterNameOfKerberos, "kerberos", "krb5.conf").mkString(HDFSHelper.systemSep),
      hadoopConfPathOfKerberos = Array(applicationHome, "hadoop-clusters", clusterNameOfKerberos, "conf").mkString(HDFSHelper.systemSep),
      trustStorePath = Array(applicationHome, "hadoop-clusters", clusterNameToConnect, "kerberos", s"$clusterNameToConnect.truststore").mkString(HDFSHelper.systemSep)
    )
  }

  def this(clusterName: String, username: String, keytabPath: String, applicationHome: String) {
    this(
      clusterName,
      username,
      keytabPath,
      clusterName,
      applicationHome
    )
  }

  def this(clusterName: String, username: String, keytabPath: String) {
    this(
      clusterName,
      username,
      keytabPath,
      HDFSHelper.classPath
    )
  }

  /**
   * 使用内置用户 robot_data / sre.bigdata 登录集群，只能在服务器内部使用该方法
   *
   * @param clusterName
   * @param applicationHome
   */
  def this(clusterName: String, applicationHome: String) = {
    this(clusterName, HDFSHelper.internalUsername, Array(applicationHome, "hadoop-clusters", clusterName, "kerberos", s"${HDFSHelper.internalUsername}.$clusterName.keytab").mkString(HDFSHelper.systemSep), applicationHome)
    println(s"====> HDFSHelper.internalUsername = ${HDFSHelper.internalUsername}")
  }

  def this(clusterName: String) = {
    this(clusterName, HDFSHelper.classPath)
    println(s"====> HDFSHelper.classPath = ${HDFSHelper.classPath}")
  }

  def loadDefaultConfigurations: Configuration = {
    val hadoopConfFiles = if (hadoopConfPathToConnect != null) {
      new File(hadoopConfPathToConnect).listFiles().map(_.getAbsolutePath)
    } else {
      Array[String]()
    }
    loadConfigurations(hadoopConfFiles)
  }

  def loadConfigurations(hadoopConfFiles: Array[String]): Configuration = {
    val conf: Configuration = HDFSUtils.initConfiguration(hadoopConfFiles: _*)
    if (kerberosEnable) {
      KerberosAuthentication.initKerberosENV(conf, username, keytabPath,
        krb5confPath, useSubjectCredsOnly, debug)
    }
    conf
  }

  private def defaultFileSystem(): FileSystem = {
    val hadoopConfFiles = if (hadoopConfPathToConnect != null) {
      new File(hadoopConfPathToConnect).listFiles().map(_.getAbsolutePath)
    } else {
      Array[String]()
    }
    newFileSystem(hadoopConfFiles)
  }

  /**
   * 获取 Kerberos 认证的 FileSystem
   *
   * @return
   */
  def newFileSystem(hadoopConfFiles: Array[String]): FileSystem = {
    val conf: Configuration = HDFSUtils.initConfiguration(hadoopConfFiles: _*)
    if (kerberosEnable) {
      val kerberosConf: Configuration =
        if (hadoopConfPathToConnect == hadoopConfPathOfKerberos) {
          conf
        }
        else {
          val kerberosConfFiles = if (hadoopConfPathOfKerberos != null) {
            new File(hadoopConfPathOfKerberos).listFiles().map(_.getAbsolutePath)
          } else {
            Array[String]()
          }
          HDFSUtils.initConfiguration(kerberosConfFiles: _*)
        }
      ugi = KerberosAuthentication.initKerberosENV(kerberosConf, username, keytabPath,
        krb5confPath, useSubjectCredsOnly, debug)
    }
    if (debug) {
      import scala.collection.JavaConverters._
      println(s"============== Final Params ==============")
      conf.getFinalParameters.asScala.foreach(println)
      println(s"============== Hadoop Properties ==============")
      conf.iterator().asScala.foreach {
        e => println(s"${e.getKey}: ${e.getValue}")
      }
    }
    // SSL config，instead property `ssl.client.truststore.location` in "ssl-client.xml"
    println(s"====> javax.net.ssl.trustStore: $trustStorePath")
    System.setProperty("javax.net.ssl.trustStore", trustStorePath)
    ugi.doAs(new PrivilegedAction[FileSystem] {
      override def run(): FileSystem = FileSystem.get(conf)
    })
  }

  def currentUser: UserGroupInformation = ugi

  def runInKerberosContext[T](execute: => T): T = {
    ugi.doAs(new PrivilegedAction[T] {
      override def run(): T = execute
    })
  }

}
