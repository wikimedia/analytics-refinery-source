package org.wikimedia.analytics.refinery.spark.utils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.security.MessageDigest
import java.io.{ObjectInputStream, ObjectOutputStream, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.codec.binary.Hex

object HdfsFileFingerprintWriter {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: HdfsFileFingerprintWriter <hdfs_path>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .getOrCreate()

    try {
      apply(args(0))
    } finally {
      spark.stop()
    }
  }

  def apply(hdfsPath: String): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Get all files in the specified folder
    val files = listFiles(fs, new Path(hdfsPath))

    // Compute one digest per task on the executors, instead of looping
    // through every file on the driver. A failed task then retries one
    // file, not the whole directory.
    val results = if (files.isEmpty) {
      Seq.empty[(String, String)]
    } else {
      val serializableConf = new SerializableHadoopConfiguration(
        spark.sparkContext.hadoopConfiguration
      )
      val filePaths = files.map(_.toString)

      spark.sparkContext
        .parallelize(filePaths, filePaths.length)
        .map { pathStr =>
          val path = new Path(pathStr)
          val taskFs = FileSystem.get(path.toUri, serializableConf.value)
          (path.getName, calculateSHA256(taskFs, path))
        }
        .collect()
        .sortBy(_._1) // keep manifest rows in the same deterministic order as before
        .toSeq
    }

    // Write manifest file in standard format so that
    // a consumer can verify via `sha256sum -c SHA256SUMS`
    val manifestPath = new Path(hdfsPath, "SHA256SUMS")
    val writer = new PrintWriter(fs.create(manifestPath))
    try {
      results.foreach { case (path, hash) =>
        writer.println(s"$hash  $path")
      }
    } finally {
      writer.close()
    }

    println(s"Created fingerprint manifest at: ${manifestPath}")
  }

  private def listFiles(fs: FileSystem, path: Path): Seq[Path] = {
    val files = ArrayBuffer[Path]()

    // order filestatus objects so that listing is deterministic
    val fileStatuses = fs.listStatus(path).sortBy(_.getPath.getName)
    fileStatuses.foreach { status =>
      // ignore hidden files or files like _SUCCESS
      if (status.isFile && !status.getPath.getName.startsWith("_") &&
        !status.getPath.getName.startsWith(".")) {
        files.append(status.getPath)
      }
    }

    files
  }

  def calculateSHA256(fs: FileSystem, path: Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val buffer = new Array[Byte](65536)

    val input = fs.open(path)
    try {
      var bytesRead = input.read(buffer)
      while (bytesRead != -1) {
        digest.update(buffer, 0, bytesRead)
        bytesRead = input.read(buffer)
      }
    } finally {
      input.close()
    }

    Hex.encodeHexString(digest.digest())
  }
}

/** Carries a Hadoop [[Configuration]] to the executors.
  *
  * `Configuration` is `Writable`, not `Serializable`. This wrapper keeps the
  * settings of the driver, so each task can open the same file system.
  *
  * The class is top level on purpose. A class inside an `object` can hold a
  * reference to that object.
  */
class SerializableHadoopConfiguration(
    @transient private var conf: Configuration
) extends Serializable {

  def value: Configuration = conf

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    conf.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    conf = new Configuration(false)
    conf.readFields(in)
  }
}
