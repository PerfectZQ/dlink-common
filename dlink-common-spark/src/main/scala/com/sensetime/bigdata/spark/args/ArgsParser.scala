package com.sensetime.bigdata.spark.args

import com.sensetime.bigdata.tool.CustomGsonKit
import org.apache.commons.cli.{HelpFormatter, Options}

/**
 * The abstraction of args parser
 * <p>
 * Note: abstract class can have parameterised constructor, but trait do not.
 *
 * @author zhangqiang
 * @since 2021/1/26 17:14
 */
abstract class ArgsParser(val args: Array[String]) extends Serializable {

  @transient protected var options: Options = _

  // abstract class constructor, will execute when implemented class constructed. trait will return null directly.
  options = defineOptions
  parse(args)

  /**
   * Define the options
   *
   * @return
   */
  protected def defineOptions: Options

  /**
   * The args parse logic
   *
   * @param args
   */
  protected def parse(args: Array[String]): Unit

  /**
   * Print usage messages
   *
   * @param width
   * @param cmdLineSyntax
   * @param header
   * @param footer
   * @param autoUsage
   */
  def printHelp(width: Int = 100,
                cmdLineSyntax: String = "",
                header: String = "-" * 100,
                footer: String = "-" * 100,
                autoUsage: Boolean = false): Unit = {
    new HelpFormatter().printHelp(width, cmdLineSyntax, header, options, footer, autoUsage)
  }

  /**
   * Print with JSON format
   *
   * @return
   */
  override def toString: String = {
    CustomGsonKit.newCustomGsonBuilder().create().toJson(this)
  }

}
