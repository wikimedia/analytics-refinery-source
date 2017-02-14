/**
 * Historical Projectcounts Raw
 *
 * Generate historical projectcounts-raw data by aggregating pagecounts-raw data.
 *
 * The resulting data set starts Dec 2007 and ends Aug 2016, and has an hourly
 * granularity. The columns are (year, month, day, hour, domainAbbrev, viewCount).
 * domainAbbrev is the webstatscollector legacy domain code: en, de.m.voy, etc.
 * The data set is partitioned by year, meaning the output is distributed in folders
 * named year=2007, year=2008, etc. Each folder contains a single gzipped TSV file.
 * The overall size of the data set is around ~350MB.
 *
 * Usage with spark-submit (or spark-shell):
 *
 *     spark-submit \
 *         --class org.wikimedia.analytics.refinery.job.HistoricalProjectcountsRaw \
 *         /path/to/read/pagecounts-raw \
 *         /path/to/write/projectcounts-raw
 */

package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.spark.Accumulator
import org.apache.log4j.LogManager

object HistoricalProjectcountsRaw {

    // Command line argument object.
    case class Config(
        sourceDir: String = "hdfs://analytics-hadoop/wmf/data/archive/pagecounts-raw",
        destinationDir: String = "hdfs://analytics-hadoop/wmf/data/archive/projectcounts-raw"
    )

    // Command line parser definition.
    val argsParser = new OptionParser[Config]("Historical Projectcounts Raw") {
        head("Historical Projectcounts Raw", "")
        note("Generate historical projectcounts-raw by aggregating pagecounts-raw data.")
        arg[String]("<sourceDir>").text("Root directory of the pagecounts-raw data set.")
        arg[String]("<destinationDir>").text("Directory where to write the output files.")
        help("help").text("Print this text and exit.")
    }

    // Helper type to use as key for aggregation.
    type Group = (Int, Int, Int, Int, String)

    /**
     * Read the directory tree of 1 year of the pagecounts-raw data set,
     * and parse it into an RDD with a format suitable for aggregation.
     * Note that part of the data is parsed from the source file names.
     */
    def parsePagecountsRaw(
        sourceDir: String,
        sqlContext: SQLContext,
        parsingErrors: Accumulator[Long]
    ): RDD[(Group, Long)] = {

        // Read all files into an RDD with the format: (absoluteFilePath, dataLine)
        import sqlContext.implicits._
        val rawData = sqlContext.read.
            text(sourceDir + "/*/*.gz").
            select(input_file_name, $"value").
            rdd

        // Parse year, month, day and hour from the file name,
        // and parse domainAbbrev and viewCount from the line.
        // The resulting format is: ((year, month, day, hour, domainAbbrev), viewCount)
        val fileNameRE = """.*/(\d{4})/\d{4}-(\d{2})/pagecounts-\d{6}(\d{2})-(\d{2})\d{4}.gz""".r
        val dataLineRE = """([^\s]+)\s[^\s]+\s([^\s]+)\s[^\s]+""".r
        rawData.flatMap{ row =>
            val (filePath, dataLine) = (row.getString(0), row.getString(1))
            filePath match {
                case fileNameRE(year, month, day, hour) =>
                    dataLine match {
                        case dataLineRE(domainAbbrev, viewCount) =>
                            Seq((
                                (year.toInt, month.toInt, day.toInt, hour.toInt, domainAbbrev),
                                viewCount.toLong
                            ))
                        case _ =>
                            parsingErrors.add(1)
                            Array.empty[(Group, Long)]
                    }
                case _ => throw new Exception("File name can not be parsed: " + filePath)
            }
        }
    }

    /**
     * Aggregate pagecounts-raw dataset into projectcounts-raw
     * (discard the article title dimension and sum up view counts across wikis).
     * To reduce the amount of data that is redistributed across the cluster,
     * the data is pre-aggregated. Read more about this inline.
     */
    def aggregateProjectcountsRaw(
        pagecountsRaw: RDD[(Group, Long)]
    ): RDD[(Group, Long)] = {
        // Pre-aggregate within partitions.
        // We know that all elements in a partition have the same year, month, day and hour,
        // because they come from the same hourly file. If we pre-aggregate them within the
        // partition we reduce its size considerably from ~1M elements to ~800 elements.
        // This reduces the data that needs to be hashed and redistributed when aggregating
        // across the whole data set.
        val preAggregated = pagecountsRaw.mapPartitions{ it =>
            it.foldLeft(Map.empty[Group, Long]){ case (aggregated, (group, viewCount)) =>
                aggregated + (group -> (aggregated.getOrElse(group, 0L) + viewCount))
            }.toIterator
        }

        // Aggregate (fully) across all partitions and sort.
        // This function's output format remains the same as the input.
        // Coalesce resulting data into 1 partition to force 1 single output file.
        preAggregated.reduceByKey(_ + _).sortBy(_._1, true, 1)
    }

    /**
     * Write the aggregated projectcounts-raw data in a format recognizable by hive.
     * The output is partitioned in yearly directories named year=2007, year=2008, etc.
     * Each directory contains 1 gzipped TSV file holding all data for that year.
     * The columns in the file do not contain the year.
     * Note: DataFrame.write.partitionBy() does not work with option("codec", "gzip")
     * nor with option("compression", "gzip"), it only generates snappy files. That's
     * why this function writes the output files one by one sequentially. This is OK
     * performance-wise, because the bulk of the job has already been done in a
     * distributed fashion, and the writing is only 10 small files (~350MB overall).
     */
    def writeProjectcountsRaw(
        projectcountsRaw: RDD[(Group, Long)],
        destinationDir: String,
        sqlContext: SQLContext
    ) = {

        // Define the data schema.
        val schema = StructType(Seq(
            StructField("month", IntegerType, nullable = false),
            StructField("day", IntegerType, nullable = false),
            StructField("hour", IntegerType, nullable = false),
            StructField("domain_abbrev", StringType, nullable = false),
            StructField("view_count", LongType, nullable = false)
        ))

        // Transform data into Rows.
        val rows = projectcountsRaw.map{
            case ((_, month, day, hour, domainAbbrev), viewCount) =>
                Row(month, day, hour, domainAbbrev, viewCount)
        }

        // Write the output.
        val df = sqlContext.createDataFrame(rows, schema)
        df.write.
            format("csv").
            option("delimiter", "\t").
            option("codec", "gzip").
            save(destinationDir)
    }

    def main(args: Array[String]) {
        argsParser.parse(args, Config()) match {
            case Some(params) => {
                val log = LogManager.getRootLogger

                // Initial Spark setup.
                val conf = new SparkConf().setAppName("HistoricalProjectcountsRaw")
                val sc = new SparkContext(conf)
                val sqlContext = new SQLContext(sc)

                // Accumulator to keep track of parsing errors.
                val parsingErrors = sc.accumulator(0L)

                // Write 1 file for each year in the data set.
                (2007 to 2016).foreach{ year =>
                    log.info(s"Processing data for $year...")

                    // Read pagecounts-raw
                    val pagecountsRaw = parsePagecountsRaw(
                        params.sourceDir + s"/$year",
                        sqlContext,
                        parsingErrors
                    )

                    // Aggregate.
                    val projectcountsRaw = aggregateProjectcountsRaw(pagecountsRaw)

                    // Write projectcounts-raw.
                    writeProjectcountsRaw(
                        projectcountsRaw,
                        params.destinationDir + s"/year=$year",
                        sqlContext
                    )
                }

                // Print number of parsing errors for sanity check.
                log.info(s"Done! (parsing errors: $parsingErrors)")
            }
            // Arguments are bad, error message will have been displayed.
            case None => sys.exit(1)
        }
    }
}
