package com.sensetime.bigdata.spark.args

import com.sensetime.bigdata.tool.CustomGsonKit
import org.apache.commons.cli.{HelpFormatter, Options}

/**
 * The abstraction of args parser
 *
 * @author zhangqiang
 * @since 2021/1/26 17:14
 */
trait ArgsParser {

  protected var args: Array[String]

  parse(args)

  @transient protected val options: Options = defineOptions

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
