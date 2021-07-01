package com.sensetime.bigdata.hadoop.hdfs

import org.apache.hadoop.conf.Configuration

/**
 * @author zhangqiang
 * @since 2021/7/1 8:51 下午
 */
object ConfigurationUtil {

  /**
   * 获取 Configuration
   *
   * @param disableCache 是否禁用 FileSystem CACHE
   * @return
   */
  def disableCache(configuration: Configuration = new Configuration(true), disableCache: Boolean = false): Configuration = {
    configuration.setBoolean("fs.hdfs.impl.disable.cache", disableCache)
    configuration
  }

}
