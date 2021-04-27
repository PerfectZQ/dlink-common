package com.sensetime.bigdata.hadoop

import org.apache.hadoop.fs.FileStatus

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

/**
 * @author zhangqiang
 * @since 2021/4/27 16:21
 */
package object extension {

  implicit class FileStatusExt(fileStatus: FileStatus) {

    /**
     * 获取字符串路径，处理特殊字符
     *
     * @return
     */
    def getStringPath: String = {
      val rawPath = fileStatus.getPath.toUri.getRawPath
      URLDecoder.decode(fuckingSpecialCharacters(rawPath), StandardCharsets.UTF_8.name())
    }

    /**
     * 优雅的解决特殊字符问题
     *
     * @param str
     * @return
     */
    private def fuckingSpecialCharacters(str: String): String = {
      // 将 + 号 URLEncode 为 %2B， 然后在 URLDecode 时还原回 +， 否则会错误的还原成空格
      str.replaceAll("\\+", "%2B")
    }

  }

}
