package org.wikimedia.analytics.refinery.job


import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lower, first}
import org.apache.spark.SparkConf
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

import scala.collection.immutable._

/**
 * Job to pivot data.
 *
 * The data is expected to be stored in an N-columns format where:
 *   - column N-1 is the column to pivot
 *   - column N is the value to fill the pivoted columns with.
 *
 * The job will fail if it finds duplicated set of values for the dimension columns.
 */
object DataPivoter extends LogHelper with ConfigHelper {

    val AcceptedFormats: Seq[String] = Seq("csv", "parquet", "avro")

    /**
      * Class storing parameter values
      */
    case class Config(
        source_directory: String,
        destination_directory: String,
        reader_format: String = "csv",
        reader_options: Map[String, String] = Map(
            "delimiter" -> "\t",
            "header" -> "true"
        ),
        writer_format: String = "csv",
        writer_options: Map[String, String] = Map(
            "delimiter" -> "\t",
            "header" -> "true",
            "compression" -> "none"
        ),
        fill_value: String = "0"
    )

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting due to exception.")
                sys.exit(1)
        }
        log.info("Loaded configuration:\n" + prettyPrint(config))
        config
    }

    object Config {
        val defaults: Config = Config(
            source_directory = "/dummy/source/folder",
            destination_directory = "/dummy/dest/folder"
        )
        val propertiesDoc: ListMap[String, String] = ListMap(
            "source_directory" ->
                "Directory where the source data to be pivoted is located.",
            "reader_format" ->
                s"""The file format to use to read the source data.
                   |Should be one of ${AcceptedFormats.mkString(", ")}.
                   |Default: ${defaults.reader_format}""".stripMargin,
            "reader_options" ->
                s"""The source read-options to be passed to spark, in key-value format
                   |k1:v1,k2:v2. Default: ${defaults.reader_options}""".stripMargin,
            "destination_directory" ->
                "Directory where to write the pivoted result file.",
            "writer_format" ->
                s"""The file format to use to write the pivoted data.
                   |Should be one of${AcceptedFormats.mkString(", ")}.
                   |Default: ${defaults.writer_format}""".stripMargin,
            "writer_options" ->
                s"""The destination write-options to be passed to spark, in key-value format k1:v1,k2,v2.
                   |Default: ${defaults.writer_options}""".stripMargin,
            "fill_value" ->
                s"""The value used to fill-in empty report-cells after pivoting.
                   |Default: ${defaults.fill_value}""".stripMargin
        )
        val usage: String =
            """
              |Job to pivot data.
              |The data is expected to be stored in an N-columns format where:
              | - column N-1 is the column to pivot
              | - column N is a value to fill the pivoted columns with
              |
              |The job will fail if it finds duplicated set of values for the dimension columns.
              |
              |Example:
              |  spark3-submit --master local[2] \
              |  --class org.wikimedia.analytics.refinery.job.DynamicPivoter \
              |  /path/to/refinery-job.jar \
              |  --source_directory=/example/source/directory \
              |  --destination_directory=/example/destination/directory
              |"""

    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }
        val config = loadConfig(args)
        // Make sure to log in the console when launched from Airflow
        addConsoleLogAppender()
        val conf = new SparkConf().setAppName(s"DynamicPivoter")
        val spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()
        sys.exit(apply(spark)(config) match {
            case Success(_) => 0
            case Failure(_) => 1
        })
    }

    def apply(spark: SparkSession)(config: Config): Try[Unit] = Try {
        try {
            // Read source data using provided config
            val sourceData = spark.read
                .options(config.reader_options)
                .format(config.reader_format)
                .load(config.source_directory)

            val pivotedData = pivotDataframe(sourceData, config.fill_value)

            // Write pivoted data using provided config
            pivotedData
                .write
                .mode(SaveMode.Overwrite)
                .options(config.writer_options)
                .format(config.writer_format)
                .save(config.destination_directory)

            Success
        } catch {
            case e: Exception =>
                log.error(s"Failed pivoting dataset at ${config.source_directory}", e)
                // Rethrow error after logging to force failure
                throw e
        }
    }

    def pivotDataframe(df: DataFrame, emptyValue: String): DataFrame = {
        // if a value in the pivot column has varied casing, it will pivot but fail to insert
        // later (because insert column names are not case sensitive, example: WikiViz and Wikiviz)
        // to prevent this, transform the data to normalize case to the first found value
        val pivotCol = df.col(df.columns(df.columns.length -2))
        val fixNamesDF = df.groupBy(lower(pivotCol).as("lowercase")).agg(first(pivotCol).as("canonical"))
        val fixedDF = df.join(fixNamesDF, lower(pivotCol).equalTo(fixNamesDF.col("lowercase")))

        // Prepare columns reused in data pivotal
        val valueColName = fixedDF.columns.dropRight(2).last
        val valueCol = fixedDF.col(valueColName)
        val toPivotCol = fixedDF.col("canonical")
        val noChangeCols = fixedDF.columns.dropRight(4).foldLeft(Seq.empty[Column])(
            (colList, colName) => colList :+ fixedDF.col(colName))

        // Optimize computation by caching the data
        fixedDF.cache()

        // Assert that there is no duplication in dimension-columns set of values
        if (fixedDF.count() > fixedDF.select(noChangeCols :+ toPivotCol:_*).distinct().count()) {
            throw new IllegalStateException(
                "The dimension-columns contains duplicate while distinct sets of values are expected."
            )
        } else {
            log.info("No duplication in dimension-columns values - Pivoting the data.")
        }

        // Pivot dataframe
        fixedDF
            .groupBy(noChangeCols:_*)
            .pivot(toPivotCol)
            .agg(first(valueCol).as(valueColName))
            .na.fill(emptyValue)
            .orderBy(noChangeCols:_*)
            .coalesce(1)
    }
}
