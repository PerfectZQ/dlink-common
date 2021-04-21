package com.sensetime.bigdata.spark.args.examples

import com.sensetime.bigdata.spark.args.ArgsParser
import org.apache.commons.cli.{BasicParser, CommandLine, Options}

/**
 * @author zhangqiang
 * @since 2021/4/12 14:59
 */
case class MetaIncrementalEventConsumerArgs(override val args: Array[String]) extends ArgsParser(args) {

  var indexPrefix: String = _
  var indexSuffix: String = _
  var internalPrincipal: String = _
  var topics: Array[String] = _
  var resetStrategy: String = _
  var groupId: String = _
  var autoCommit: java.lang.Boolean = _
  var partitions: Int = _
  var duration: Int = _
  var bootstrapServers: String = _

  override protected def defineOptions: Options = {
    new Options()
      .addOption("i", "indexPrefix", true, "ES索引前缀名称")
      .addOption("s", "indexSuffix", true, "ES索引后缀名称")
      .addOption("u", "username", true, "用户名")
      .addOption("t", "topics", true, "audit_log 在 kafka 中的 topics")
      .addOption("r", "reset", true, "消费策略")
      .addOption("g", "groupId", true, "消费者组")
      .addOption("c", "autoCommit", true, "自动提交")
      .addOption("p", "partitions", true, "分区数量")
      .addOption("d", "duration", true, "batch大小，秒")
      .addOption("b", "bootstrapServers", true, "ip1:port1,ip2:port2")
  }

  override def parse(args: Array[String]): Unit = {
    val cmd: CommandLine = new BasicParser().parse(options, args)
    if (cmd.hasOption("h")) printHelp()
    // es hdfs dir mirror index prefix
    indexPrefix = cmd.getOptionValue("i", "raw_hdfs_meta_test_")
    indexSuffix = cmd.getOptionValue("s", "")
    internalPrincipal = cmd.getOptionValue("u", "sre.bigdata")
    topics = cmd.getOptionValues("t")
    if (topics == null || topics.length == 0) topics = Array("hadoop_hdfs_audit")
    resetStrategy = cmd.getOptionValue("r", "earliest")
    // "group.id" -> "ranger_meta_event" / "ranger_test"
    groupId = cmd.getOptionValue("g", "ranger_test")
    autoCommit = java.lang.Boolean.parseBoolean(cmd.getOptionValue("c", "false"))
    partitions = cmd.getOptionValue("p", "-1").toInt
    duration = cmd.getOptionValue("d", "1").toInt
    bootstrapServers = cmd.getOptionValue("b", "10.198.22.61:9092,10.198.23.124:9092,10.198.20.218:9092")
  }

}
