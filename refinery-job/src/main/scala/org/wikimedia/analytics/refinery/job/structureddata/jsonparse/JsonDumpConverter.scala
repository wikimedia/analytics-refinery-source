package org.wikimedia.analytics.refinery.job.structureddata.jsonparse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SaveMode, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import scopt.OptionParser

import scala.reflect.{ClassTag, Manifest, classTag}

/**
 * This job converts a structured data json dump (json formatted dump containing all structured data
 * information stored as in wikibase, currently wikidata or commons), into either parquet or avro
 * with a similar but optimized for data treatment schema.
 *
 * It uses classes defined in JsonClasses.scala to parse and the TableClasses
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
 *     --class org.wikimedia.analytics.refinery.job.structureddata.jsonparse.JsonDumpConverter \
 *     /path/to/refinery-job.jar \
 *     -i /wmf/data/raw/wikidata/dumps/all_json/20200120 \
 *     -o /wmf/data/wmf/wikidata/page/snapshot=20200120 \
 *     -p wikidata \
 *     -f parquet \
 *     -n 512
 *
 */
object JsonDumpConverter {

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
        projectType: String = "wikidata",
        numPartitions: Int = 512,
        debug: Boolean = false
    )

    /**
     * CLI Option Parser for job parameters (fill-in Params case class)
     */
    val argsParser = new OptionParser[Params]("") {
        head("Structured Data Json Dump Converter", "")
        help("help") text "Prints this usage text"

        opt[String]('i', "json-dump-path") required() valueName "<path>" action { (x, p) =>
            p.copy(jsonDumpPath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text "Path to structured-data json dump to convert"

        opt[String]('o', "output-path") required() valueName "<path>" action { (x, p) =>
            p.copy(outputPath = if (x.endsWith("/")) x else x + "/")
        } text "Path to output structured-data parquet result."

        opt[String]('f', "output-format") optional() action { (x, p) =>
            p.copy(outputFormat = x)
        } validate { x =>
            if (! Seq("avro", "parquet").contains(x))
                failure("Invalid output format - can be avro or parquet")
            else
                success
        } text "Output file format, avro or parquet. Defaults to parquet"

        opt[String]('p', "project-type") optional() action { (x, p) =>
            p.copy(projectType = x)
        } validate { x =>
            if (! Seq("wikidata", "commons").contains(x))
                failure("Invalid output format - can be wikidata , commons")
            else
                success
        } text "Structured data project to parse, wikidata or commons. Defaults to wikidata"

        opt[Int]('n', "num-partitions") optional() action { (x, p) =>
            p.copy(numPartitions = x)
        } text "Number of partitions to use (output files). Defaults to 512"

        opt[Unit]("debug").action((_, c) =>
            c.copy(debug = true)).text("debug mode -- spark logs added to applicative logs (VERY verbose)")

    }

    /**
     * Method parsing a json entity into a {Wikidata,Commons}JsonEntity class
     * Note: 'value' fields are kept as json as handling the complex types is not yet done.
     * @param json the json to parse
     * @return the parsed JsonEntityType (CommonsJsonEntity or WikidataJsonEntity)
     */
    def parseEntity[JsonEntityType](json: String)(implicit tagjs: Manifest[JsonEntityType]): JsonEntityType = {
        parse(json)
            // Values as complex objects are not yet worked out.
            // Instead we transform them as a json string and write it this way
            .transformField { case JField("value", value) => ("value", JString(compact(render(value)))) }
            .extract[JsonEntityType]
    }

    /**
      * Method converting raw lines of wikidata/commons dumps to a dataframe.
      * Note: Lines with less than 10 characters are removed  - this is needed
      * to force the removal of json array characters in dumps).
      */
    def jsonLinesToTable[TableEntityType: ClassTag](
        spark: SparkSession,
        jsonLines: RDD[String],
        jsonConverter: (String) => TableEntityType
    )(
        implicit tableEntityEncoder: Encoder[TableEntityType]
    ): DataFrame = {
        import spark.implicits._

        jsonLines
          .filter(_.length > 10)                        // Remove array surrounding data
          .map(_.stripLineEnd.replaceAll("},$", "}"))   // Remove colon after each item
          .map(jsonConverter)                           // Parse json and Convert to Entity
          .toDS                                         // To dataset
          .toDF                                         // To Dataframe
    }

    /**
      * Wikidata and Commons converters (json parsing and entity creation)
      * Those values are useful for reusability in testing noticeably.
      */
    val wikidataJsonConverter = (json: String) => new WikidataEntity(JsonDumpConverter.parseEntity[WikidataJsonEntity](json))
    val commonsJsonConverter = (json: String) => new CommonsEntity(parseEntity[CommonsJsonEntity](json))

    def main(args: Array[String]): Unit = {
        argsParser.parse(args, Params()) match {
            case Some(params) =>

                val project = params.projectType

                val spark = SparkSession
                    .builder()
                    .appName(project.capitalize + "JsonDumpConverter")  // e.g WikidataJsonDumpConverter
                    .getOrCreate()

                import spark.implicits._

                val jsonLines = spark.sparkContext.textFile(params.jsonDumpPath) // Read text file

                // The DataFrames could not be made generic due to field name differences and absence of few fields
                val tableData = project match {
                    case "wikidata" => jsonLinesToTable[WikidataEntity](spark, jsonLines, wikidataJsonConverter)
                    case "commons" => jsonLinesToTable[CommonsEntity](spark, jsonLines, commonsJsonConverter)
                }

                //Write dataframe overwriting if exists
                tableData.repartition(params.numPartitions).write.mode(SaveMode.Overwrite).format(params.outputFormat).save(params.outputPath)

            case None => sys.exit(1) // If args parsing fail (parser prints nice error)
        }
    }

}
