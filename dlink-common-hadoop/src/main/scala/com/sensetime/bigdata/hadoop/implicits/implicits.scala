package com.sensetime.bigdata.hadoop

import org.apache.hadoop.fs._
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
import scala.util.Random

/**
 * @author zhangqiang
 * @since 2021/4/12 14:31
 */
package object implicits {

  /**
   * Extension for [[FileSystem]]
   *
   * @param fileSystem
   */
  implicit class ExtensionFileSystem(fileSystem: FileSystem) {

    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
     * 取指定目录的样例文件
     *
     * @param f                 指定文件夹/文件路径
     * @param num               取样例的个数
     * @param sizeThreshold     取样例文件大小超过阈值的
     * @param recursive         是否递归
     * @param fixed             如果为 true: 取样例文件时，如果原始数据没有变化，且取随机样例数不变，每次调用返回的数据完全相同
     * @param sampleSpaceFactor 样例空间因子，如果大于 0，则取前 Min(num * sampleSpaceFactor, fileCount) 个文件作为样本空间
     * @return
     */
    def listSampleFiles(f: Path, num: Int, sizeThreshold: Int = 1024, recursive: Boolean = false,
                        fixed: Boolean = false, sampleSpaceFactor: Int = 0): List[LocatedFileStatus] = {

      val fileCount = fileSystem.getContentSummary(f).getFileCount.asInstanceOf[Int]
      logger.info(s"===> Path: $f, fileCount: $fileCount")
      val random = if (fixed) new Random(seed = Integer.MAX_VALUE) else Random
      val sampleSpaceSize: Int = if (sampleSpaceFactor > 0) Math.min(num * sampleSpaceFactor, fileCount) else fileCount
      logger.info(s"===> sampleSpaceSize: $sampleSpaceSize")

      val actualSampleSpaceElems = new RemoteIterator[LocatedFileStatus]() {
        private val iteratorStack: util.Stack[RemoteIterator[LocatedFileStatus]] = new util.Stack[RemoteIterator[LocatedFileStatus]]
        private var curIterator: RemoteIterator[LocatedFileStatus] = fileSystem.listLocatedStatus(f)
        private var curFile: LocatedFileStatus = _
        private var currentCapacity: Int = 0

        @throws[IOException]
        override def hasNext: Boolean = {
          while (curFile == null) {
            if (currentCapacity == sampleSpaceSize) return false
            else if (curIterator.hasNext) handleFileStat(curIterator.next)
            else if (!iteratorStack.empty) curIterator = iteratorStack.pop
            else return false
          }
          true
        }

        /**
         * Process the input stat.
         * If it is a file, return the file stat.
         * If it is a directory, traverse the directory if recursive is true;
         * ignore it if recursive is false.
         *
         * @param stat input status
         * @throws IOException if any IO error occurs
         */
        @throws[IOException]
        private def handleFileStat(stat: LocatedFileStatus): Unit = {
          if (stat.isFile) { // file
            curFile = stat
          }
          else if (recursive) { // directory
            iteratorStack.push(curIterator)
            curIterator = fileSystem.listLocatedStatus(stat.getPath)
          }
        }

        @throws[IOException]
        override def next: LocatedFileStatus = {
          var result: LocatedFileStatus = null
          while (hasNext && result == null) {
            result = curFile
            curFile = null
            if (result.getLen > sizeThreshold) {
              logger.info(s"===> currentCapacity : $currentCapacity")
              currentCapacity = currentCapacity + 1
            } else {
              logger.info(s"===> Iterator elem skip. ")
              result = null
            }
          }
          result
          /*
          if (hasNext) {
            logger.info(s"===> currentCapacity : $currentCapacity")
            var result: LocatedFileStatus = curFile
            curFile = null
            if (result.getLen > sizeThreshold) {
              currentCapacity = currentCapacity + 1
              return result
            } else {
              result = null
              while (hasNext && result == null) {
                result = next
              }
              return result
            }
          }
          throw new NoSuchElementException("No more entry in " + f)
          */
        }
      }
        .toIterator
        .toList

      val actualSampleSpaceSize = actualSampleSpaceElems.size
      logger.info(s"===> actualSampleSpaceSize: $actualSampleSpaceSize")
      val sampleNum: Int = Math.min(num, actualSampleSpaceSize)
      logger.info(s"===> sampleNum: $sampleNum")
      val sampleSeq = random.distinctRandomSeq(sampleNum, actualSampleSpaceSize)
      logger.info(s"===> sampleSeq: $sampleSeq")

      sampleSeq.map(index => actualSampleSpaceElems(index)).toList
    }
  }

  /**
   * Extension for [[RemoteIterator]]
   *
   * @param remoteIterator
   * @tparam T
   */
  implicit class ExtensionRemoteIterator[T <: FileStatus](remoteIterator: RemoteIterator[T]) {

    private val logger = LoggerFactory.getLogger(this.getClass)

    def transform(): ArrayBuffer[T] = {
      val buffer: ArrayBuffer[T] = new ArrayBuffer[T]()
      while (remoteIterator.hasNext) {
        val elem = remoteIterator.next()
        if (elem != null) buffer += elem
      }
      buffer
    }

    def toIterator: Iterator[T] = {
      transform().toIterator
    }

    /**
     * Convert to parallel array
     *
     * @return
     */
    def toParArray: ParArray[T] = {
      transform().toParArray
    }

    /**
     * 取样例文件，完全随机
     *
     * @param num           取样例的个数
     * @param sizeThreshold 取样例文件大小超过阈值的
     * @param fixed         true: 取样例文件，如果原始数据没有变化，且取随机样例数不变，每次调用返回的数据完全相同
     * @return
     */
    def samples(num: Int, sizeThreshold: Int = 1024, fixed: Boolean = false): List[T] = {
      val samples: ArrayBuffer[T] = new ArrayBuffer[T]()
      val files = transform().filter(f => f.getLen > sizeThreshold)
      if (files.size <= num) {
        logger.warn(s"The number of sample files which file size is great than $sizeThreshold is ${files.size}, less or equal than expect number $num ")
        samples ++= files
      } else {
        val random = if (fixed) new Random(seed = Integer.MAX_VALUE) else Random
        samples ++= random.distinctRandomSeq(num, files.size).map(randIndex => files(randIndex))
        /*
        Range(0, num).foreach { _ =>
          if (files.nonEmpty) {
            val randIndex = random.nextInt(files.size)
            samples += files(randIndex)
            files.remove(randIndex)
          }
        }
        */
      }
      samples.toList
    }

  }


  implicit class HexBytes(bytes: Array[Byte]) {

    private val HEX16 = "0123456789abcdef"

    def toHex16String: String = {
      val sb = new StringBuffer(bytes.length * 2)
      bytes.foreach { b =>
        // 取出这个字节的高4位，然后与0x0f与运算，得到一个0-15之间的数据，通过HEX.charAt(0-15)即为16进制数
        sb.append(HEX16.charAt((b >> 4) & 0x0f))
        // 取出这个字节的低位，与0x0f与运算，得到一个0-15之间的数据，通过HEX.charAt(0-15)即为16进制数
        sb.append(HEX16.charAt(b & 0x0f))
      }
      sb.toString
    }
  }

  implicit class ExtensionRandom(random: Random) {
    /**
     * 生成 $size 位，元素不重复的随机序列
     *
     * @param size       Size of generated sequence
     * @param upperBound Returns a pseudorandom, uniformly distributed int value between 0
     *                   (inclusive) and the specified `upperBound` value (exclusive)
     * @return
     */
    def distinctRandomSeq(size: Int, upperBound: Int): Seq[Int] = {
      if (size > upperBound) throw new IllegalArgumentException("size can't great than upperBound")
      val linkedHashSet = new mutable.LinkedHashSet[Int]
      while (linkedHashSet.size < size) {
        linkedHashSet += random.nextInt(upperBound)
      }
      linkedHashSet.toList
    }

  }

}