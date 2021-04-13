package com.sensetime.bigdata.common

import java.math.BigInteger
import java.security.MessageDigest

/**
 * @author zhangqiang
 * @since 2020/5/25 11:27
 */
object HashUtils {
  
  def md5(text: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    new BigInteger(1, md.digest(text.getBytes("UTF-8"))).toString(16)
  }

}
