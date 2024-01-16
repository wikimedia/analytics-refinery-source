package org.wikimedia.analytics.refinery.spark.deequ

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.BufferedOutputStream
import scala.util.{Failure, Try}

trait TextWriter extends Writer

/**
 * Writes a blob of text to HDFS.
 *
 */
protected class HdfsTextWriter(outputPath: String, text: String, spark: SparkSession) extends TextWriter {
    private val logger: Logger = Logger.getLogger(getClass)
    private var _outputPath: String = outputPath

    /**
     * Save `text` to `outputPath` in HDFS.
     */
    def save(): Unit = {
        val fileName = new Path(output)
        val fileSystem = fileName.getFileSystem(spark.sparkContext.hadoopConfiguration)

        Try {
            if (text.nonEmpty) {
                val output = fileSystem.create(fileName);
                val os = new BufferedOutputStream(output)
                os.write(text.getBytes)
                os.close()
            }
        } match {
            case Failure(exception) =>
                logger.error(s"Save operation failed with exception: $exception")
            case _ => // Do nothing in case of success
        }
    }

    /**
     * Set the output path
     * @param path
     * @return
     */
    def output(path: String): HdfsTextWriter = {
        this._outputPath = path
        this
    }

    /**
     * Get the output path
     * @return
     */
    def output: String = this._outputPath
}

object HdfsTextWriter {
  def apply(outputPath: String, text: String, spark: SparkSession) =
      new HdfsTextWriter(outputPath, text, spark)
}