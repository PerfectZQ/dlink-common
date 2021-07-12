package com.sensetime.common.test

import com.sensetime.bigdata.hadoop.hdfs.FileSystemProxyFactory
import org.apache.hadoop.fs.Path

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer

/**
 * @author zhangqiang
 * @since 2021/7/12 12:59 下午
 */
object FileSystemProxyTest {
  def main(args: Array[String]): Unit = {
    val fileSystem = new FileSystemProxyFactory().getProxyInstance
    val fsDataInputStream = fileSystem.open(new Path("pom.xml"))
    val br = new BufferedReader(new InputStreamReader(fsDataInputStream))
    val buffer: ArrayBuffer[String] = new ArrayBuffer[String]()
    var path: String = br.readLine()
    while (path != null) {
      buffer += path
      path = br.readLine()
      println(path)
    }
    br.close()
    fileSystem.close()
  }
}
