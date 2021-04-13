package com.sensetime.bigdata.common

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.tika.config.TikaConfig
import org.apache.tika.io.{TemporaryResources, TikaInputStream}
import org.apache.tika.metadata.Metadata
import org.apache.tika.mime.MediaType
import org.slf4j.LoggerFactory

import java.io._

object Files {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 取输入流的前 size 个字节
   *
   * @param inputStream
   * @param size
   * @return
   */
  def getMagicHeaderInputStream(inputStream: InputStream, size: Int = 1024): ByteArrayInputStream = {
    val magicHeader = new Array[Byte](size)
    try {
      inputStream.read(magicHeader)
    } catch {
      case e: IOException => e.printStackTrace()
        logger.error("getMagicHeaderInputStream", e)
    } finally {
      inputStream.close()
    }
    new ByteArrayInputStream(magicHeader)
  }

  def detectHdfsFileContentType(path: Path, fromHead: Boolean = true)(implicit fileSystem: FileSystem): MediaType = {
    val fsDataInputStream: FSDataInputStream = fileSystem.open(path, 1024)
    if (fromHead) {
      detectContentType(getMagicHeaderInputStream(fsDataInputStream))
    } else {
      detectContentType(fsDataInputStream)
    }
  }

  def detectLocalFileContentType(file: File, fromHead: Boolean = true): MediaType = {
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(file))
    if (fromHead) {
      detectContentType(getMagicHeaderInputStream(bufferedInputStream))
    } else {
      detectContentType(bufferedInputStream)
    }
  }

  def detectBytesContentType(bytes: Array[Byte]): MediaType = {
    val byteArrayInputStream = new ByteArrayInputStream(bytes)
    detectContentType(byteArrayInputStream)
  }


  /**
   * Detect content-type from input stream
   *
   * @see [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types]]
   * @param inputStream
   * @return
   */
  def detectContentType(inputStream: InputStream): MediaType = {
    var mediaType: MediaType = MediaType.EMPTY
    var tmp: TemporaryResources = null
    var tis: TikaInputStream = null
    try {
      // Avoid org.apache.tika.exception.ZeroByteFileException
      if (inputStream.available() > 0) {
        val metadata = new Metadata()
        val detector = TikaConfig.getDefaultConfig.getDetector
        tmp = new TemporaryResources()
        tis = TikaInputStream.get(inputStream, tmp)
        mediaType = detector.detect(tis, metadata)
      }
    } catch {
      case e: Exception => logger.error("detectContentType", e)
    } finally {
      if (tis != null)
        tis.close()
      if (tmp != null)
        tmp.close()
    }
    mediaType
  }


  /**
   * 根据 Content-Type 获取文件后缀名成
   *
   * @param contentType
   * @return
   */
  @Deprecated
  def getSuffixType(contentType: String): String = {
    // Unknown binary file
    if (contentType == null || "application/octet-stream".equalsIgnoreCase(contentType)) {
      ""
    } else if ("application/avro".equalsIgnoreCase(contentType)) {
      ".avro"
    } else if (contentType.contains("plain;")) {
      ".plainText"
    } else if (contentType.contains("/")) {
      s".${contentType.split("/")(1)}"
    } else {
      s".$contentType"
    }
    // TODO: type detect can more specific and precise
  }

}
