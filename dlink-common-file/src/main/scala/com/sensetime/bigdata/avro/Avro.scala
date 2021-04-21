package com.sensetime.bigdata.avro

import com.sensetime.bigdata.common.Files
import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.file.{DataFileReader, MagicHeaderDataFileReader, MagicHeaderDatumReader}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader
import org.apache.hadoop.fs.{AvroFSInput, FileStatus, FileSystem}
import org.apache.tika.mime.MediaType
import org.slf4j.LoggerFactory

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util
import scala.collection.mutable.ArrayBuffer

object Avro {

  private val logger = LoggerFactory.getLogger(getClass)

  import scala.collection.JavaConverters._

  def isBytesTypeField(field: Field): Boolean = {
    val fieldSchema = field.schema()
    val typ = fieldSchema.getType
    if (Type.UNION.equals(typ)) {
      fieldSchema.getTypes.asScala
        .map(unionSchema => unionSchema.getType)
        .contains(Type.BYTES)
    }
    else typ.equals(Type.BYTES)
  }

  /**
   * 通过 schema 获取存储数据的字段名称
   *
   * @param schema
   * @return
   */
  def locateDataFieldsOfAvroSchema(schema: Schema): Array[String] = {
    val bytesFieldNames = schema.getFields.asScala
      .filter(field => isBytesTypeField(field))
      .map(field => field.name())
      .toArray
    bytesFieldNames
  }


  /**
   * 解析 HDFS 中 avro 文件的 schema
   *
   * @param fileStatus
   * @param fs
   * @return
   */
  def getAvroSchema(fileStatus: FileStatus)(implicit fs: FileSystem): Schema = {
    val fileLen = fileStatus.getLen
    if (fileLen == 0) throw new IllegalArgumentException(s"${fileStatus.getPath.getName}'s length can't be 0")
    var fsInput: AvroFSInput = null
    var fileReader: DataFileReader[GenericRecord] = null
    var schema: Schema = null
    try {
      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
      val fsDataInputStream = fs.open(fileStatus.getPath)
      fsInput = new AvroFSInput(fsDataInputStream, fileStatus.getLen)
      fileReader = new DataFileReader(fsInput, reader)
      schema = fileReader.getSchema
      logger.info(schema.toString(true))
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("====> getAvroSchema", e)
    } finally {
      if (fileReader != null)
        fileReader.close()
      if (fsInput != null)
        fsInput.close()
    }
    schema
  }

  /**
   * 解析 Avro Schema 信息
   *
   * @param file
   * @return
   */
  def getAvroSchema(file: File): Schema = {
    val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
    var fileReader: DataFileReader[GenericRecord] = null
    var schema: Schema = null
    try {
      fileReader = new DataFileReader(file, reader)
      schema = fileReader.getSchema
      logger.debug(schema.toString(true))
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("", e)
    } finally {
      if (fileReader != null)
        fileReader.close()
    }
    schema
  }

  /**
   * If targetPath is null, default deserialize avro file records to ./${avro_file_name}_deser/${record_index}
   *
   * @param file
   * @param top 取前 top 条记录
   * @return
   */
  def deserializeLocalAvro(file: File, targetPath: String = null, top: Int = Integer.MAX_VALUE): Unit = {
    logger.info(s"====> Begin deserialize ${file.getAbsolutePath}")
    val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
    var fileReader: DataFileReader[GenericRecord] = null
    try {
      fileReader = new DataFileReader(file, reader)
      val schema: Schema = fileReader.getSchema
      logger.info(s"====> Schema: \n${schema.toString(true)}")
      val dataFieldNames = locateDataFieldsOfAvroSchema(schema)
      val record: GenericRecord = new GenericData.Record(schema)
      var recordIndex = 0
      var flag = true
      while (fileReader.hasNext && flag) {
        recordIndex += 1
        flag = recordIndex < top
        fileReader.next(record)
        dataFieldNames.foreach { dataFieldName =>
          val bin = record.get(dataFieldName)
          if (bin != null) {
            val binBytes = bin.asInstanceOf[ByteBuffer].array()
            val magicBytes = util.Arrays.copyOfRange(binBytes, 0, 1024)
            val contentType = Files.detectBytesContentType(magicBytes)
            val suffixType = contentType.getSubtype
            val targetDir = if (targetPath != null) {
              new File(targetPath)
            } else {
              new File(file.getParentFile.getAbsolutePath + "_deser")
            }
            if (!targetDir.exists()) targetDir.mkdirs()
            val targetFile = new File(s"${targetDir.getAbsolutePath}/${file.getName}-$recordIndex.$suffixType")
            // TODO: change to NIO
            val fos = new FileOutputStream(targetFile)
            fos.write(binBytes)
            fos.flush()
            fos.close()
            logger.info(s"====> Deserialize to $targetFile finished.")
          }
        }
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        logger.error("", t)
    }
    finally {
      if (fileReader != null)
        fileReader.close()
    }
    logger.info(s"====> Finished deserialize ${file.getAbsolutePath}")
  }


  /**
   * 解析 HDFS Avro 文件前 top 条记录到本地
   *
   * @param fileStatus
   * @param targetPath
   * @param top
   * @param fs
   */
  def deserializeHDFSAvro(fileStatus: FileStatus, targetPath: String = null, top: Int = Integer.MAX_VALUE)(implicit fs: FileSystem): Unit = {
    val len = fileStatus.getLen
    val path = fileStatus.getPath
    val rawPath = path.toUri.getRawPath
    if (len > 1024) {
      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
      var seekableInput: AvroFSInput = null
      var fileReader: DataFileReader[GenericRecord] = null
      try {
        val fsDataInputStream = fs.open(fileStatus.getPath)
        seekableInput = new AvroFSInput(fsDataInputStream, len)
        fileReader = new DataFileReader[GenericRecord](seekableInput, reader)
        val schema: Schema = fileReader.getSchema
        val dataFieldNames = locateDataFieldsOfAvroSchema(schema)
        logger.info(s"====> Deserialize $rawPath, schema: \n${schema.toString(true)}")
        val record: GenericRecord = new GenericData.Record(schema)
        var recordIndex = 0
        var flag = true
        while (fileReader.hasNext && flag) {
          recordIndex += 1
          flag = recordIndex < top
          fileReader.next(record)
          dataFieldNames.foreach { dataFieldName =>
            val bin = record.get(dataFieldName)
            if (bin != null) {
              val binBytes = bin.asInstanceOf[ByteBuffer].array()
              val contentType = Files.detectBytesContentType(binBytes)
              val suffixType = contentType.getSubtype
              val targetDir = if (targetPath != null) {
                new File(targetPath)
              } else {
                new File(rawPath)
              }
              if (!targetDir.exists()) targetDir.mkdirs()
              val targetFile = new File(s"${targetDir.getAbsolutePath}/${path.getName}$recordIndex.$suffixType")
              // TODO: change to NIO
              val fos = new FileOutputStream(targetFile)
              fos.write(binBytes)
              fos.flush()
              fos.close()
              logger.info(s"====> Deserialize from $path to $targetFile finished.")
            }
          }
        }
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          logger.error(s"====> Error deserializeHDFSAvro $rawPath", t)
      } finally {
        if (fileReader != null)
          fileReader.close()
        if (seekableInput != null)
          seekableInput.close()
      }
      logger.info(s"====> Finished deserialize $rawPath")
    } else {
      logger.info(s"====> Ignore avro $rawPath, cause file size < 1024")
    }
  }


  /**
   * 检测 HDFS Avro 文件内部嵌套文件的文件类型
   *
   * @param fileStatus
   * @param top
   * @param fs
   * @return
   */
  def checkAvroEmbedContentTypesOnHDFS(fileStatus: FileStatus, top: Int = Integer.MAX_VALUE)(implicit fs: FileSystem): List[MediaType] = {
    val len = fileStatus.getLen
    val path = fileStatus.getPath
    val rawPath = path.toUri.getRawPath
    val resultSet = new ArrayBuffer[MediaType]()
    if (len > 1024) {
      logger.info(s"====> Begin checkAvroEmbedContentTypesOnHDFS $rawPath")
      val reader: DatumReader[GenericRecord] = new MagicHeaderDatumReader[GenericRecord]()
      var seekableInput: AvroFSInput = null
      var fileReader: DataFileReader[GenericRecord] = null
      try {
        val fsDataInputStream = fs.open(fileStatus.getPath)
        seekableInput = new AvroFSInput(fsDataInputStream, len)
        fileReader = new MagicHeaderDataFileReader(seekableInput, reader)
        val schema: Schema = fileReader.getSchema
        logger.info(s"====> Deserialize ${path.toUri.getRawPath}, schema: \n${schema.toString(true)}")
        val dataFieldNames = locateDataFieldsOfAvroSchema(schema)
        val record: GenericRecord = new GenericData.Record(schema)
        var recordIndex = 0
        var flag = true
        while (fileReader.hasNext && flag) {
          recordIndex += 1
          flag = recordIndex < top
          fileReader.next(record)
          dataFieldNames.foreach { dataFieldName =>
            val bin = record.get(dataFieldName)
            if (bin != null) {
              val magicHeaderThreshold = 10 * 1024
              val magicHeaderBytes = new Array[Byte](magicHeaderThreshold)
              val binByteBuffer = bin.asInstanceOf[ByteBuffer]
              val remaining = binByteBuffer.remaining()
              logger.info(s"====> The length of binByteBuffer: $remaining")
              val len = if (remaining < magicHeaderThreshold) remaining else magicHeaderThreshold
              binByteBuffer.get(magicHeaderBytes, 0, len)
              val contentType = Files.detectBytesContentType(magicHeaderBytes)
              resultSet += contentType
            }
          }
        }
      } catch {
        case t: Throwable => t.printStackTrace()
          logger.error(s"====> Error checkAvroEmbedContentTypesOnHDFS $rawPath", t)
      } finally {
        if (fileReader != null)
          fileReader.close()
        if (seekableInput != null)
          seekableInput.close()
      }
      logger.info(s"====> Finished checkAvroEmbedContentTypesOnHDFS $rawPath")
    } else {
      logger.info(s"====> Ignore avro $rawPath, cause file size < 1024 on checkAvroEmbedContentTypesOnHDFS")
    }
    resultSet.toList
  }

}
