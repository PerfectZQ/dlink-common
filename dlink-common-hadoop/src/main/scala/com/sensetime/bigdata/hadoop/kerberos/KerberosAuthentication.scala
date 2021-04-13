package com.sensetime.bigdata.hadoop.kerberos

import com.sensetime.bigdata.hadoop.hdfs.HDFSUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.slf4j.LoggerFactory

object KerberosAuthentication {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * local develop client connect to remote HDFS cluster, just for local debug
   *
   * @param clusterName
   * @return
   */
  def initKerberosConfigurationForDebug(clusterName: String): Configuration = {
    val kerberosBasePath = "C:\\Users\\zhangqiang\\IdeaProjects\\myutils\\kerberos"
    val clusterConfBasePath = "C:\\Users\\zhangqiang\\IdeaProjects\\myutils\\cluster_confs"
    val conf = HDFSUtils.initConfiguration(
      s"""$clusterConfBasePath\\$clusterName\\confs\\core-site.xml""",
      s"""$clusterConfBasePath\\$clusterName\\confs\\hdfs-site.xml""")
    initKerberosENV(conf, "robot_data",
      s"""$kerberosBasePath\\$clusterName\\robot_data.$clusterName.keytab""",
      s"""$kerberosBasePath\\$clusterName\\krb5.conf""")
    conf
  }

  /**
   * Initialize hdfs kerberos authentication
   *
   * @param conf
   * @param principal
   * @param keytabPath
   * @param krb5confPath
   * @param useSubjectCredsOnly
   * @param debug
   */
  def initKerberosENV(conf: Configuration,
                      principal: String,
                      keytabPath: String,
                      krb5confPath: String = null,
                      useSubjectCredsOnly: Boolean = false,
                      debug: Boolean = false): UserGroupInformation = {

    this.synchronized {

      if (krb5confPath != null && krb5confPath.nonEmpty) {
        // Set krb5.conf if it can't get Kerberos realm
        System.setProperty("java.security.krb5.conf", krb5confPath)
      }
      System.setProperty("javax.security.auth.useSubjectCredsOnly", useSubjectCredsOnly.toString)
      System.setProperty("sun.security.krb5.debug", debug.toString)
      conf.set("hadoop.security.authentication", "kerberos")
      // https://hadoop.apache.org/docs/r1.0.4/core-default.html
      // core-site.xml, default true
      // conf.set("hadoop.security.token.service.use_ip", "false")

      // Reset login cache
      UserGroupInformation.reset() // For hadoop 2.7.7+
      // For hadoop 2.7.1
      //      val clazz: Class[UserGroupInformation] = Class.forName("org.apache.hadoop.security.UserGroupInformation").asInstanceOf[Class[UserGroupInformation]]
      //      val resetMethod = clazz.getDeclaredMethod("reset")
      //      resetMethod.setAccessible(true)
      //      resetMethod.invoke(null)

      // Equals UserGroupInformation.initialize(conf, true)
      UserGroupInformation.setConfiguration(conf)
      // Login currentContext
      //      UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
      //      val currentUser: UserGroupInformation = UserGroupInformation.getCurrentUser

      // Login and return UGI
      val ugi: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath)
      logger.info(s"===> Login successful for user: ${ugi.getUserName}, using keytab file: $keytabPath")
      ugi
    }

  }

}
