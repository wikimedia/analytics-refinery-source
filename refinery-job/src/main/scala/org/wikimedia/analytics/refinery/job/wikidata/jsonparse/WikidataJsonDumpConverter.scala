package org.wikimedia.analytics.refinery.job.wikidata.jsonparse

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import scopt.OptionParser

/**
 * This job converts a wikidata json dump (json formatted dump containing all wikidata information
 * stored as in wikibase), into either parquet or avro with a similar but optimized for data
 * treatment schema.
 *
 * It use classes defined in WikidataJsonClasses.scala to parse and the WikidataStructuredDataClasses
 * to convert the json a to a more data-friendly schema.
 *
 * Details of schema changes:
 *  - Maps of snaks and site-links are flattened, as their keys are referenced in the values
 *  - Some fields are renamed to prevent operator-name conflict in SQL
 *
 * Command line example:
 * spark2-submit --master yarn --driver-memory 16G --executor-memory 32G --executor-cores 4 \
 *     --conf spark.dynamicAllocation.maxExecutors=32 \
 *     --conf spark.executor.memoryOverhead=8196 \
 *     --class org.wikimedia.analytics.refinery.job.wikidata.jsonparse.WikidataJsonDumpConverter \
 *     /path/to/refinery-job.jar \
 *     -i /wmf/data/raw/mediawiki/wikidata/all_jsondumps/20200120 \
 *     -o /wmf/data/wmf/wikidata/page/snapshot=20200120 \
 *     -f parquet \
 *     -n 512
 *
 */
object WikidataJsonDumpConverter {

    @transient
    // Used for json parsing
    lazy implicit val formats = DefaultFormats

    /**
     * Class storing parameter values
     */
    case class Params(
        jsonDumpPath: String = "",
        outputPath: String = "",
        outputFormat: String = "parquet",
        numPartitions: Int = 512,
        debug: Boolean = false
    )

    /**
     * CLI Option Parser for job parameters (fill-in Params case class)
     */
    val argsParser = new OptionParser[Params]("") {
        head("Wikidata Json Dump Converter", "")
        help("help") text "Prints this usage text"

        opt[String]('i', "json-dump-path") required() valueName "<path>" action { (x, p) =>
            p.copy(jsonDumpPath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text "Path to wikidata json dump to convert"

        opt[String]('o', "output-path") required() valueName "<path>" action { (x, p) =>
            p.copy(outputPath = if (x.endsWith("/")) x else x + "/")
        } text "Path to output wikidata parquet result."

        opt[String]('f', "output-format") optional() action { (x, p) =>
            p.copy(outputFormat = x)
        } validate { x =>
            if (! Seq("avro", "parquet").contains(x))
                failure("Invalid output format - can be avro or parquet")
            else
                success
        } text "Output file format, avro or parquet. Defaults to parquet"

        opt[Int]('n', "num-partitions") optional() action { (x, p) =>
            p.copy(numPartitions = x)
        } text "Number of partitions to use (output files). Defaults to 512"

        opt[Unit]("debug").action((_, c) =>
            c.copy(debug = true)).text("debug mode -- spark logs added to applicative logs (VERY verbose)")

    }

    /**
     * Method parsing a json entity into a WikidataJsonClasses.JsonEntity class
     * Note: 'value' fields are kept as json as handling the complex types is not yet done.
     * @param json the json to parse
     * @return the parsed JsonEntity
     */
    def parseEntity(json: String): JsonEntity = {
        parse(json)
            // Values as complex objects are not yet worked out.
            // Instead we transform them as a json string and write it this way
            .transformField { case JField("value", value) => ("value", JString(compact(render(value)))) }
            .extract[JsonEntity]
    }

    def main(args: Array[String]): Unit = {
        argsParser.parse(args, Params()) match {
            case Some(params) =>

                val spark = SparkSession
                    .builder()
                    .appName("WikidataJsonDumpConverter")
                    .getOrCreate()

                import spark.implicits._

                val df = spark.sparkContext.
                    textFile(params.jsonDumpPath).
                    filter(_.length > 10).                      // Remove array surrounding data
                    map(_.stripLineEnd.replaceAll("},$", "}")). // Remove colon after each item
                    map(parseEntity).                           // Parse json into WikidataJson objects
                    map(je => new Entity(je)).                  // WikidataJson to Wikidata-graph objects
                    toDS.                                       // to DataSet
                    toDF                                        // to DataFrame

                //Write dataframe overwriting if exists
                df.repartition(params.numPartitions).write.mode(SaveMode.Overwrite).format(params.outputFormat).save(params.outputPath)

            case None => sys.exit(1) // If args parsing fail (parser prints nice error)
        }
    }

}
