package com.sensetime.bigdata.hadoop.hdfs

import com.google.gson.GsonBuilder

import com.sensetime.bigdata.avro.Avro
import com.sensetime.bigdata.avro.Avro.locateDataFieldsOfAvroSchema
import com.sensetime.bigdata.common.Files
import com.sensetime.bigdata.hadoop.implicits.{ExtensionFileSystem, ExtensionRemoteIterator}

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.AclStatus
import org.apache.hadoop.hdfs.DistributedFileSystem

import org.apache.tika.mime.MediaType

import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

/**
 * @author zhangqiang
 */
object HDFSUtils {

  val classPath: String = getClass.getClassLoader.getResource("").getPath


  private lazy val logger = LoggerFactory.getLogger(getClass)

  // sz: hdfs://master002.hadoop-sz.data.sensetime.com:8020
  case class Meta(filePath: String,
                  storageFileTypes: Array[String],
                  storageFileSubTypes: Array[String],
                  deserializeFileTypes: Array[String],
                  deserializeFileSubTypes: Array[String],
                  fileStatus: FileStatus,
                  aclStatus: AclStatus,
                  contentSummary: ContentSummary)

  /**
   * Initialize common hdfs configurations
   *
   * @return
   */
  def initConfiguration(resources: String*): Configuration = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    resources.foreach { resource =>
      if (resource != null && resource.nonEmpty) {
        logger.info(s"====> Add resource: $resource")
        conf.addResource(new Path(resource))
      }
    }
    conf
  }

  /**
   * Initialize common hdfs configurations
   *
   * @return
   */
  def initConfigurationByIS(resources: InputStream*): Configuration = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    resources.foreach { resource =>
      if (resource != null && resource.available() > 0) {
        logger.debug(s"====> Add resource: $resource")
        conf.addResource(resource)
      }
    }
    conf
  }

  /**
   * ??????????????????????????????????????????????????????????????????????????????????????? / ??????????????? /a/b ??? /a/b/c ??????????????????????????? /a/b/c
   *
   * @param path
   * @return
   */
  def traverLongDirs(path: Path)(implicit fileSystem: FileSystem): ArrayBuffer[String] = {
    val arr = new ArrayBuffer[String]()
    val currentFileStatus = fileSystem.getFileStatus(path)
    if (currentFileStatus.isDirectory) {
      val childDirs = getChildDirs(path)
      if (childDirs.isEmpty) {
        val contentSummary: ContentSummary = fileSystem.getContentSummary(path)
        // Corresponding `hadoop fs -du -s -h /dirname`, show data size of single replication.
        val singleRepDirSize = contentSummary.getLength
        // Corresponding `dfs used`, show total storage size of real data in cluster, equals `singleRepDirSize * replicaNum`
        val totalDirSize = contentSummary.getSpaceConsumed
        val currentAclStatus = fileSystem.getAclStatus(path)
        val filePath = currentFileStatus.getPath.toUri.getPath
        val dirMeta = new GsonBuilder().create().toJson(
          //Meta(filePath, currentFileStatus, currentAclStatus, contentSummary)
        )
        arr += dirMeta
        // for debug
        println(dirMeta)
      } else arr ++= childDirs.flatMap(dirStatus => traverLongDirs(dirStatus.getPath))
    }
    arr
  }

  /**
   * ???????????????????????????????????? db
   *
   * @param currentDirLevel ??????????????????
   * @param path
   * @param fileSystem
   * @return
   */
  def extractLongDirMeta(extractor: => Unit, path: Path, currentDirLevel: Int = 1)(implicit fileSystem: FileSystem): Unit = {
    val currentFileStatus = fileSystem.getFileStatus(path)
    if (currentFileStatus.isDirectory) {
      try {
        val childDirs = getChildDirs(path)
        val filePath = currentFileStatus.getPath.toUri.getPath
        if (childDirs.isEmpty) {
          val contentSummary: ContentSummary = fileSystem.getContentSummary(path)
          val aclStatus: AclStatus = fileSystem.getAclStatus(path)
          val dirMeta = null // Meta(filePath, currentFileStatus, aclStatus, contentSummary)
          extractor
        } else {
          childDirs.toParArray.foreach { dirStatus =>
            extractLongDirMeta(extractor, dirStatus.getPath, currentDirLevel + 1)
          }
        }
      } catch {
        case e: Throwable => e.printStackTrace()
          logger.error("extractLongDirMeta", e)
      }
    }
  }

  def getChildDirs(path: Path)(implicit fs: FileSystem): ArrayBuffer[FileStatus] = {
    val arr = new ArrayBuffer[FileStatus]()
    val remoteIterator: RemoteIterator[FileStatus] = fs.listStatusIterator(path)
    while (remoteIterator.hasNext) {
      val fileStatus = remoteIterator.next()
      if (fileStatus.isDirectory) arr += fileStatus
    }
    arr
  }

  /**
   * ????????????????????????????????????????????????????????????????????????????????????
   *
   * @param path
   * @param dirProcessor ?????????????????????
   * @param fileSystem
   */
  def walksAllDirs(path: Path, dirProcessor: FileStatus => Unit)(implicit fileSystem: FileSystem): (Long, Long, Int) = {
    def processor(fileStatus: FileStatus): Unit = {
      if (fileStatus.isDirectory) dirProcessor(fileStatus)
    }

    walksAllFilesAndDirs(path, processor)
  }

  /**
   * ?????????????????????????????????????????????????????????????????????????????????????????????
   *
   * @param path
   * @param processor ???????????????/?????????????????????????????????
   * @param fileSystem
   */
  def walksAllFilesAndDirs(path: Path, processor: FileStatus => Unit)(implicit fileSystem: FileSystem): (Long, Long, Int) = {
    var totalDirsNum: Long = 0L
    var totalFilesNum: Long = 0L
    // ?????????????????????
    var maxDirDepth: Int = 0
    try {
      val currentFileStatus = fileSystem.getFileStatus(path)
      // Will process data in the end save memory on recursive function in scala ?
      processor(currentFileStatus)
      if (currentFileStatus.isDirectory) {
        totalDirsNum += 1
        maxDirDepth += 1
        val (childDirsNum, childFilesNum, dirsDepth) = fileSystem.listLocatedStatus(path)
          // Scan child paths in parallel.
          .toParArray
          .map(fileStatus => walksAllFilesAndDirs(fileStatus.getPath, processor))
          .unzip3
        totalDirsNum += childDirsNum.sum
        totalFilesNum += childFilesNum.sum
        if (dirsDepth.nonEmpty) maxDirDepth += dirsDepth.max
      } else {
        totalFilesNum += 1
      }
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("walksAllFilesAndDirs", e)
    }
    (totalDirsNum, totalFilesNum, maxDirDepth)
  }

  /**
   * ?????????????????????????????????????????????????????????????????????????????????
   *
   * @param path
   * @param fileProcessor
   * @param fileSystem
   * @return
   */
  def walksAllFiles(path: Path, fileProcessor: FileStatus => Unit)(implicit fileSystem: FileSystem): (Long, Long, Int) = {
    def processor(fileStatus: FileStatus): Unit = {
      if (!fileStatus.isDirectory) fileProcessor(fileStatus)
    }
    walksAllFilesAndDirs(path, processor)
  }

  /**
   * ??????????????????????????????????????????
   *
   * @param extractor
   * @param path
   * @param currentDirLevel
   * @param fileSystem
   */
  def extractAllDirsMeta(extractor: => Unit, path: Path, currentDirLevel: Int = 1)
                        (implicit fileSystem: FileSystem): (Long, Long) = {

    var totalDirsNum: Long = 0L
    var totalFilesNum: Long = 0L
    try {
      val currentFileStatus = fileSystem.getFileStatus(path)
      if (currentFileStatus.isDirectory) {
        val filePath = currentFileStatus.getPath.toUri.getPath
        val contentSummary: ContentSummary = fileSystem.getContentSummary(path)
        val aclStatus: AclStatus = fileSystem.getAclStatus(path)
        val dirMeta: Meta = null // Meta(filePath, currentFileStatus, aclStatus, contentSummary)
        extractor

        totalDirsNum += 1

      }
    } catch {
      case e: Throwable => e.printStackTrace()
        logger.error("extractAllDirsMeta", e)
    }
    (totalDirsNum, totalFilesNum)
  }


  /**
   * ???????????? path ?????? sampleNum ???????????????
   *
   * @param path
   * @param sampleNum
   * @param fileSystem
   * @param deserializerAvro ?????? Avro ?????????????????????
   */
  def downloadSampleData(path: Path, targetPath: String, sampleNum: Int = 5, deserializerAvro: Boolean = true)
                        (implicit fileSystem: FileSystem): Unit = {
    sampleFiles(path, sampleNum).foreach {
      fileStatus =>
        val sourcePath = fileStatus.getPath
        val path = fileStatus.getPath
        if (!deserializerAvro) {
          fileSystem.copyToLocalFile(false, sourcePath, new Path(targetPath), true)
          logger.info(s"Download ${path.getName} to $targetPath.")
        } else {
          val contentType = Files.detectHdfsFileContentType(path)
          if ("application/avro".equalsIgnoreCase(contentType.toString)) {
            Avro.deserializeHDFSAvro(fileStatus, targetPath, 1)
          } else {
            fileSystem.copyToLocalFile(false, sourcePath, new Path(targetPath), true)
            logger.info(s"Download ${path.getName} to $targetPath.")
          }
        }
    }
  }

  /**
   * ??? HDFS ?????????????????????????????????
   *
   * @param path
   * @param bufferSize
   * @param fileSystem
   */
  def deserializeAvroToByteArrayIS(path: Path, bufferSize: Int = 1024)
                                  (implicit fileSystem: FileSystem): InputStream = {
    val top = 1 // ??? avro ?????? top ????????????????????? 1
    val fsDataInputStream = fileSystem.open(path, bufferSize)
    val fileStatus = fileSystem.getFileStatus(path)
    val len = fileStatus.getLen
    val rawPath = path.toUri.getRawPath
    val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
    var seekableInput: AvroFSInput = null
    var fileReader: DataFileReader[GenericRecord] = null

    seekableInput = new AvroFSInput(fsDataInputStream, len)
    fileReader = new DataFileReader[GenericRecord](seekableInput, reader)
    val schema: Schema = fileReader.getSchema
    val dataFieldNames = locateDataFieldsOfAvroSchema(schema)
    logger.info(s"Deserialize $rawPath, schema: \n${schema.toString(true)}")
    val record: GenericRecord = new GenericData.Record(schema)
    var recordIndex = 0
    var flag = true
    var byteArrayInputStream: ByteArrayInputStream = new ByteArrayInputStream(Array[Byte]())
    while (fileReader.hasNext && flag) {
      recordIndex += 1
      flag = recordIndex < top
      fileReader.next(record)
      dataFieldNames.foreach { dataFieldName =>
        val bin = record.get(dataFieldName)
        if (bin != null) {
          val binBytes = bin.asInstanceOf[ByteBuffer].array()
          byteArrayInputStream = new ByteArrayInputStream(binBytes)
        }
      }
    }
    if (fileReader != null)
      fileReader.close()
    if (seekableInput != null)
      seekableInput.close()

    byteArrayInputStream
  }

  /**
   * ???????????????????????????????????????
   *
   * @param fileStatus
   * @param fromHead
   * @param fileSystem
   * @return
   */
  def getContentTypes(fileStatus: List[FileStatus], fromHead: Boolean = true)(implicit fileSystem: FileSystem): Map[FileStatus, MediaType] = {
    fileStatus.toParArray
      .filter(fileStatus => fileStatus.isFile)
      .map { fileStatus =>
        val mediaType = Files.detectHdfsFileContentType(fileStatus.getPath, fromHead).getBaseType
        (fileStatus, mediaType)
      }
      .toArray
      .toMap
  }

  /**
   * ???????????????????????????????????????
   *
   * @param fileStatus
   * @param fromHead
   * @param fileSystem
   * @return
   */
  def getFileStorage(fileStatus: List[FileStatus], fromHead: Boolean = true)(implicit fileSystem: FileSystem): Array[MediaType] = {
    fileStatus.toParArray
      .filter(fileStatus => fileStatus.isFile)
      .map(fileStatus =>
        Files.detectHdfsFileContentType(fileStatus.getPath, fromHead))
      .toSet
      .toArray
  }

  /**
   * ??????????????????????????? num ???????????????
   *
   * @param path
   * @param num
   * @param recursive     ??????????????????????????????????????????
   * @param fixElems      ????????????????????????????????????????????????????????????????????????????????????????????? [3,7,1,5]????????????????????? [3,7,1,5]
   * @param sizeThreshold ???????????????????????????????????? Byte
   * @param fileSystem
   * @return
   */
  def sampleFiles(path: Path, num: Int = 5, recursive: Boolean = true, fixElems: Boolean = false,
                  sizeThreshold: Int = 1024, sampleSpaceFactor: Int = 0)
                 (implicit fileSystem: FileSystem): List[FileStatus] = {
    fileSystem
      .listSampleFiles(path, num, recursive = recursive, sizeThreshold = sizeThreshold,
        fixed = fixElems, sampleSpaceFactor = sampleSpaceFactor)
  }

}
