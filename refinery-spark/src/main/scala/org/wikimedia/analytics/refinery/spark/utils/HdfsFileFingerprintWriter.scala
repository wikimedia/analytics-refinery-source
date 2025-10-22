package org.wikimedia.analytics.refinery.spark.utils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.security.MessageDigest
import java.io.{BufferedInputStream, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.codec.binary.Hex

object HdfsFileFingerprintWriter {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: HdfsFileFingerprintWriter <hdfs_path>")
      System.exit(1)
    }

    apply(args(0))
  }

  def apply(hdfsPath: String): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    try {
      // Get all files in the specified folder
      val files = listFiles(fs, new Path(hdfsPath))

      // Create fingerprints and store results
      val results = ArrayBuffer[(String, String)]()

      files.foreach { file =>
        val fingerprint = calculateSHA256(fs, file)
        val relativePath = file.getName
        results.append((relativePath, fingerprint))
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

    } finally {
      spark.stop()
    }
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
    val buffer = new Array[Byte](8192)

    val input = new BufferedInputStream(fs.open(path))
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
