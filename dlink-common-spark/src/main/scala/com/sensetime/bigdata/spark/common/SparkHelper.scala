package com.sensetime.bigdata.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.slf4j.LoggerFactory

import java.nio.file.{Path, Paths}

/**
 * @author zhangqiang
 * @since 2020/5/28 12:27
 */
object SparkHelper {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private val sparkMaster: String = System.getProperty("spark.master")

  private lazy val buddhaBlessing =
    """
			|..................... 我佛慈悲 .....................
			|
			|                      _oo0oo_
			|                     o6666666o
			|                     66" . "66
			|                     (| -_- |)
			|                     0\  =  /0
			|                   ___/`---'\___
			|                 .' \\|     |// '.
			|                / \\|||  :  |||// \
			|               / _||||| -卍-|||||- \
			|              |   | \\\  -  /// |   |
			|              | \_|  ''\---/''  |_/ |
			|              \  .-\__  '-'  ___/-. /
			|            ___'. .'  /--.--\  `. .'___
			|         ."" '<  `.___\_<|>_/___.' >' "".
			|        | | :  `- \`.;`\ _ /`;.`/ - ` : | |
			|        \  \ `_.   \_ __\ /__ _/   .-` /  /
			|    =====`-.____`.___ \_____/___.-`___.-'=====
			|                      `=---='
			|
			|................. 佛祖开光, 永无八哥 ..................
		""".stripMargin


  /**
   * 获取 sparkSession
   *
   * @param appName    spark app name
   * @param conf       sparkConf
   * @param enableHive enable hive support
   * @return sparkSession
   */
  def getSparkSession(appName: String,
                      conf: SparkConf = new SparkConf,
                      enableHive: Boolean = true): SparkSession = {
    logger.warn(buddhaBlessing)
    val builder = SparkSession.builder().config(conf).appName(appName)
    if (sparkMaster == null) {
      builder.master("local[*]")
      logger.warn("Using local[*] as spark master since `spark.master` is not set.")
      // Just for winutils.exe
      val localHadoopPath = Paths.get(System.getProperty("user.dir"), "hadoop").toString
      System.setProperty("hadoop.home.dir", localHadoopPath)
      logger.warn(s"Set hadoop.home.dir = $localHadoopPath")
    }
    if (enableHive) {
      // yarn client 模式下需要指定，否则会莫名其妙的读file:/etc/spark2/2.6.4.0-91/0/hive-site.xml
      // 而不是 classpath 中的 hive-site.xml，这样就导致 hive warehouse 指定为 local
      // 指定 spark-sql 读取/存储 hive 表数据的位置，默认会在local当前目录下创建 spark-warehouse
      // builder.config("spark.sql.warehouse.dir", "hdfs:///apps/hive/warehouse")
      builder.enableHiveSupport()
    }
    builder.getOrCreate
  }


  def getStreamingContext(appName: String,
                          batchDuration: Duration,
                          conf: SparkConf = new SparkConf): StreamingContext = {

    logger.warn(buddhaBlessing)
    conf.setAppName(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    if (sparkMaster == null) {
      conf.setMaster("local[*]")
      logger.warn("Using local[*] as spark master since `spark.master` is not set.")
    }
    new StreamingContext(conf, batchDuration)
  }

}
