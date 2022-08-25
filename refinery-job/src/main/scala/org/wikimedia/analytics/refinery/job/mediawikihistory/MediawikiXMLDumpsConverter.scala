package org.wikimedia.analytics.refinery.job.mediawikihistory

import java.beans.Transient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.wikihadoop.newapi.MediawikiXMLRevisionToJSONInputFormat

import scala.collection.immutable.ListMap
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

// Needed for JSON parsing and manipulation
import org.json4s._
import org.json4s.jackson.JsonMethods._


/**
 * Mediawiki XML Dumps converter to parquet or json.
 *
 * This job uses [[org.wikimedia.wikihadoop.newapi.MediawikiXMLRevisionToJSONInputFormat]]
 * to make Spark parse parse XML-dumps revisions into JSON-strings. The JSON is then either
 * saved as-is or in parquet.
 *
 * This job purpose is to convert multiple XML-dumps organised in wiki-named folders in a
 * base folder. The job converts dumps files in parallel, trying to parallelize small-wikis
 * together and keeping big wikis alone.
 *
 * Example launch command (using joal settings):
 *
 * spark2-submit \
 *     --master yarn \
 *     --executor-memory 8G \
 *     --driver-memory 4G \
 *     --executor-cores 2 \
 *     --conf spark.dynamicAllocation.maxExecutors=64 \
 *     --class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiXMLDumpsConverter \
 *     /home/joal/code/refinery-source/target/refinery-job-0.76-SNAPSHOT.jar \
 *     --xml_dumps_base_path  hdfs:///user/joal/wmf/data/raw/mediawiki/xmldumps/20180801/pages-meta-history \
 *     --output_base_path hdfs:///user/joal/wmf/data/wmf/mediawiki/wikitext/snapshot=2018-01
 */

object MediawikiXMLDumpsConverter extends ConfigHelper {

    @transient
    lazy val log: Logger = Logger.getLogger(this.getClass)

    // This value and the next function are used to define the number of
    // files to generate for a wiki based on the input data size.
    // See makeJobConfigs
    @Transient
    lazy val twoPows: Seq[Long] = new Range(0, 20, 1).map(p => math.pow(2, p).toLong)

    def nextTwoPow(v: Long): Long = {
        twoPows.filter(_ > v).head
    }

    // Schema of the revisions to output
    @transient
    lazy val wikiTextStructure = StructType(Seq(
        StructField("wiki_db", StringType, nullable = false),

        StructField("page_id", LongType, nullable = false),
        StructField("page_namespace", IntegerType, nullable = false),
        StructField("page_title", StringType, nullable = false),
        StructField("page_redirect_title", StringType, nullable = false),
        StructField("page_restrictions", ArrayType(StringType, containsNull = false), nullable = false),

        StructField("user_id", LongType, nullable = false),
        StructField("user_text", StringType, nullable = false),

        StructField("revision_id", LongType, nullable = false),
        StructField("revision_parent_id", LongType, nullable = false),
        StructField("revision_timestamp", StringType, nullable = false),
        StructField("revision_minor_edit", BooleanType, nullable = false),
        StructField("revision_comment", StringType, nullable = false),
        StructField("revision_text_bytes", LongType, nullable = false),
        StructField("revision_text_sha1", StringType, nullable = false),
        StructField("revision_text", StringType, nullable = false),
        StructField("revision_content_model", StringType, nullable = false),
        StructField("revision_content_format", StringType, nullable = false)
    ))

    /**
     * Conversion configuration for a job.
     * This class facilitates pre-configuring jobs
     * in order to try to bundle them in a nice
     * parallelizable way.
     *
     * @param xmlInputPath The path of the XML files to convert
     * @param outputPath The path of the converted output file to write
     * @param numberOutputPartitions The number of output files to write
     * @param outputFormat The output format (should be json or parquet)
     */
    case class JobConfig(
        xmlInputPath: String,
        outputPath: String,
        numberOutputPartitions: Int,
        outputFormat: String
    )

    /**
     * Builds a list of JobConfig objects, one for each directory contained in xmlInputBasePath
     * and present in projectsToWork (if defined).
     *
     * The number of output partitions chosen for a project is the first power-of-2 number
     * bigger than (input-size * 5 / hdfs-block-size). The factor 5 is an approximation of the
     * bz2 compression ratio. The idea is to try get output files having roughly the size of
     * one hdfs block.
     *
     * @param hadoopConfiguration The hadoop configuration to access HDFS
     * @param xmlInputBasePath The path of the folders containing XML dumps (by-project folders)
     * @param outputBasePath The path where to output per-project converted data
     * @param projectsToWork The optional set of projects to work (should match folders names).
     *                       Undefined option means all-projects
     * @param outputFormat The output format for the conversion (should be json or parquet)
     * @return The sequence of JobConfigs
     */
    def makeJobConfigs(
        hadoopConfiguration: Configuration,
        xmlInputBasePath: String,
        outputBasePath: String,
        projectsToWork: Option[Set[String]],
        outputFormat: String
    ): Seq[JobConfig] = {

        val xmlInputBaseHadoopPath = new Path(xmlInputBasePath)
        val hdfs = xmlInputBaseHadoopPath.getFileSystem(new Configuration(hadoopConfiguration))
        val defaultBlockSize = hdfs.getDefaultBlockSize(xmlInputBaseHadoopPath)

        if (hdfs.exists(xmlInputBaseHadoopPath)) {
            // Get all files/directories in input-path
            val inputSubfolders = hdfs.listStatus(xmlInputBaseHadoopPath).toList.filter(_.isDirectory)

            // For each subfolder directory in projectsToWork (or projectsToWork.isEmpty),
            // make a JobConfig
            inputSubfolders.flatMap(dumpDirHadoopFileStatus => {
                val dumpDirHadoopPath = dumpDirHadoopFileStatus.getPath
                if (projectsToWork.isEmpty || projectsToWork.get.contains(dumpDirHadoopPath.getName)) {
                    val dumpFilesHadoopPaths = hdfs.listStatus(dumpDirHadoopPath).toList
                    val dumpTotalSize = dumpFilesHadoopPaths.map(_.getLen).sum

                    val numberOutputPartitions = nextTwoPow(dumpTotalSize * 5 / defaultBlockSize).toInt

                    Seq(JobConfig(
                        dumpDirHadoopPath.toString,
                        s"$outputBasePath/wiki_db=${dumpDirHadoopPath.getName}",
                        numberOutputPartitions,
                        outputFormat
                    ))
                } else Seq.empty
            })
        }
        else Seq.empty
    }

    /**
     * Heuristic function grouping JobConfigs so that the number of partitions to work
     * in parallel doesn't exceed maxParallelJobs.
     *
     * To do so it builds a Sequence of JobConfigs to run in parallel (Sequence of JobConfigs).
     * For every new JobConfig in the list, make it single if its size is bigger than maxParallelJobs,
     * or try to associate it to one of the already bundled jobs, so that the sum of their output
     * partitions is smaller than maxParallelJobs.
     *
     * @param configs The Sequence of JobConfig to bundle
     * @param maxParallelJobs The maximum number of job-partitions to try to bundle together
     * @return The bundled JobConfigs
     */
    def optimizeParallelJobConfigs(
        configs: Seq[JobConfig],
        maxParallelJobs: Int
    ): Seq[Seq[JobConfig]] = {
        configs.foldLeft(Seq.empty[Seq[JobConfig]])(
            (jobConfigBundles: Seq[Seq[JobConfig]], jobConfig: JobConfig) => {

                /**
                 * Internal recursive function trying to associate jobConfig (available in scope)
                 * to an already existing jobConfigBundle. It does so by recursively deconstructing
                 * bundlesStillToCheck  to reconstruct alreadyCheckedBundles, possibly with jobConfig
                 * being contained in it
                 * @param bundlesToCheck The bundles still to check
                 * @param checkedBundles The bundles already checked
                 * @return The new bundles list, containing jobConfig either as a new bundle at the end,
                 *         or inside an existing bundle
                 */
                def optimizeRecursively(
                    bundlesToCheck: Seq[Seq[JobConfig]],
                    checkedBundles: Seq[Seq[JobConfig]] = Seq.empty
                ): Seq[Seq[JobConfig]] = {
                    bundlesToCheck match {
                        // If no more bundlesToCheck, jobConfig becomes the first member of a new bundle
                        case Nil => checkedBundles :+ Seq(jobConfig)
                        // Deconstruct bundlesToCheck
                        case bundle :: restOfBundlesToCheck =>
                            // if the sum of partitions of (bundle + jobConfig) is smaller than maxParallelJobs
                            // Put jobConfig in bundle and return
                            val bundleNumberPartitions = bundle.map(_.numberOutputPartitions).sum
                            if (bundleNumberPartitions + jobConfig.numberOutputPartitions <= maxParallelJobs) {
                                (checkedBundles :+ (bundle :+ jobConfig)) ++ restOfBundlesToCheck
                            } else {
                                // bundle is already too full, put it in checkedBundles and recurse on the
                                // rest of bundles to check
                                optimizeRecursively(restOfBundlesToCheck, checkedBundles :+ bundle)
                            }
                    }
                }

                // If current jobConfig has more partitions than maxParallelJobs,
                // work it a its own bundle.
                if (jobConfig.numberOutputPartitions >= maxParallelJobs) {
                    jobConfigBundles :+ Seq(jobConfig)
                } else {
                    // Try to associate current jobConfig to already existing bundles
                    optimizeRecursively(jobConfigBundles)
                }
            }
        )
    }

    /**
     * Processes a JobConfig: Parse the files in the input-folder
     * (XML-Dumps format expected), repartition the resulting revisions
     * in the configured number of partitions, and write them
     * to the configured output-path in the configured output format.
     * @param spark The spark session to use
     * @param config The JobConfig to process
     */
    def convertWikiDump(spark: SparkSession, config: JobConfig): Boolean = {
        try {
            val sc: SparkContext = spark.sparkContext
            // Setup XML-Dumps Hadoop InputFormat
            val wikiDumpJson = sc.newAPIHadoopFile(
                config.xmlInputPath,
                classOf[MediawikiXMLRevisionToJSONInputFormat],
                classOf[LongWritable],
                classOf[Text],
                new Configuration(sc.hadoopConfiguration))

            // Extract a Spark-Row out of the parsed JSON
            // then remove collocated duplicate revisions
            val wikitextRows = wikiDumpJson.map{
                case (_, text) =>
                    val json = parse(text.toString)
                    val revId = (json \ "id").values.asInstanceOf[BigInt].toLong
                    (revId, Row(
                        (json \ "page" \ "wiki").values.asInstanceOf[String],

                        (json \ "page" \ "id").values.asInstanceOf[BigInt].toLong,
                        (json \ "page" \ "namespace").values.asInstanceOf[BigInt].toInt,
                        (json \ "page" \ "title").values.asInstanceOf[String],
                        (json \ "page" \ "redirect").values.asInstanceOf[String],
                        (json \ "page" \ "restrictions").values.asInstanceOf[List[String]],

                        (json \ "user" \ "id").values.asInstanceOf[BigInt].toLong,
                        (json \ "user" \ "text").values.asInstanceOf[String],

                        revId,
                        (json \ "parent_id").values.asInstanceOf[BigInt].toLong,
                        (json \ "timestamp").values.asInstanceOf[String],
                        (json \ "minor").values.asInstanceOf[Boolean],
                        (json \ "comment").values.asInstanceOf[String],
                        (json \ "bytes").values.asInstanceOf[BigInt].toLong,
                        (json \ "sha1").values.asInstanceOf[String],
                        (json \ "text").values.asInstanceOf[String],
                        (json \ "model").values.asInstanceOf[String],
                        (json \ "format").values.asInstanceOf[String]
                    ))
            }.mapPartitions(it => {
                new Iterator[Row] {
                    var previousRevId: Option[Long] = None
                    override def hasNext: Boolean = {
                        it.dropWhile(t => previousRevId.contains(t._1))
                        it.hasNext
                    }
                    override def next(): Row = {
                        val (revId, row) = it.next
                        previousRevId = Some(revId)
                        row
                    }
                }
            })

            // Make a dataframe using defined schema and write it
            spark.
                createDataFrame(wikitextRows, wikiTextStructure).
                repartition(config.numberOutputPartitions).
                write.
                mode(SaveMode.Overwrite).
                format(config.outputFormat).
                save(config.outputPath)

            log.info(s"Success processing ${config.xmlInputPath}")
            true
        } catch {
            case e: Throwable =>
                log.error(s"Error Processing config ${config.xmlInputPath}: " + e.getMessage)
                false
        }
    }

    /**
     * Writes an empty file working as a flag (_SUCCESS by default) in folder.
     * @param hadoopConfiguration The hadoop configuration to use for HDFS
     * @param folder the folder where to write the flag
     */
    def writeSuccessFlag(
        hadoopConfiguration: Configuration,
        folder: String,
        flagName: String = "_SUCCESS"
    ): Unit = {
        val flagPath = if (folder.endsWith("/")) s"$folder$flagName" else s"$folder/$flagName"
        val hadoopFlagPath = new Path(flagPath)
        val hdfs = hadoopFlagPath.getFileSystem(new Configuration(hadoopConfiguration))
        hdfs.create(hadoopFlagPath).close()
        log.info(s"Success flag $flagPath written")
    }

    /**
     * Case class storing arguments parameters, used with [[ConfigHelper]].
     */
    case class Params(
        xml_dumps_base_path: String,
        output_base_path: String,
        projects_restriction: Option[Seq[String]] = None,
        max_parallel_jobs: Int = 128,
        output_format: String = "avro"
    )

    object Params {
        // This is just used to ease generating help message with default values.
        // Required parameters are set to dummy values.
        val default = Params("", "")

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "xml_dumps_base_path" ->
                """Path to xml-dumps.  This directory is expected to contain directories
                  |of individual wiki-project, containing xml-dump files.""",
            "output_base_path" ->
                """Base path of output data. Completed converted dumps are expected to be
                  |found here in wiki-project subdirectories prefixed with 'wiki_db=' to
                  |facilitate hive partitioning.""",
            "projects_restriction <project1,project2>" ->
                s"List of wiki-projects to convert (all if not set)",
            "max_parallel_jobs" ->
                s"""Maximum number of job-partitions to work in parallel. This defines how many jobs
                   |can be run in parallel, and should match the number of executors of your spark job.
                   |Default: ${default.max_parallel_jobs}""",
            "output_format" ->
                s"""Output format the xml-dumps will be converted to. Should be one of json, parquet or avro.
                   |Default: ${default.output_format}"""
        )

        val usage: String =
            """
              |XML-Dumps -> Parquet/JSON
              |
              |Given an input base path expectectd to contain XML-Dumps project-folders, this will
              |convert all (or part) of projects dumps into parquet or JSON format.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiXMLDumpsConverter refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                 /etc/refinery/convert_mediawiki_page_history_dumps.properties \
              |   # Override and/or set other configs on the CLI
              |   --xml_dumps_base_path         /wmf/data/raw/mediawiki/xmldumps/20180901 \
              |   --output_base_path            /wmf/data/wmf/mediawiki/wikitext_history/snapshot=2018-09
              |"""
    }


    def main(args: Array[String]) {
        // Argument parsing
        if (args.contains("--help")) {
            println(help(Params.usage, Params.propertiesDoc))
            sys.exit(0)
        }
        val params = try {
            configureArgs[Params](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        log.info("Loaded configuration:\n" + prettyPrint(params))

        // Initial setup - Spark, SQLContext
        val conf = new SparkConf()
            .setAppName("XMLDumpsConverter")
            .set("spark.sql.parquet.compression.codec", "snappy")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryoserializer.buffer", "512k")
            // Bump default numbers for various settings to better fit the job (big scale)
            // See https://wikitech.wikimedia.org/wiki/Analytics/Systems/Cluster/Spark#Spark_tuning_for_big_jobs
            .set("spark.stage.maxConsecutiveAttempts", "10")
            .set("spark.rpc.io.serverTreads", "64")
            .set("spark.shuffle.file.buffer", "1MB")
            .set("spark.unsafe.sorter.spill.reader.buffer.size", "1MB")
            .set("spark.file.transferTo", "false")
            .set("spark.shuffle.unsafe.file.output.buffer", "5MB")
            .set("spark.io.compression.lz4.blockSize", "512KB")
            .set("spark.shuffle.service.index.cache.size", "2048")
            .set("spark.shuffle.registration.timeout", "2m")
            .set("spark.shuffle.registration.maxAttempts", "5")


        val spark = SparkSession.builder().config(conf).getOrCreate()

        // Build and optimize configs for parallel execution based on input size
        val configs = makeJobConfigs(
            spark.sparkContext.hadoopConfiguration,
            params.xml_dumps_base_path,
            params.output_base_path,
            params.projects_restriction.map(_.toSet),
            params.output_format
        )
        val jobConfigBundles = optimizeParallelJobConfigs(configs, params.max_parallel_jobs)
        log.info(s"Found ${configs.length} projects to work in ${jobConfigBundles.length} groups")

        // jobConfigBundles are worked sequentially, with every JobConfig
        // of a bundle being worked in parallel

        val results = jobConfigBundles.flatMap(bundle => {
            val parBundle = bundle.par
            parBundle.tasksupport = new ForkJoinTaskSupport(
                new ForkJoinPool(params.max_parallel_jobs)
            )
            parBundle.map(config => (config, convertWikiDump(spark, config)))
        })

        // If all job result are positive, write success flag, else error with message
        if (results.forall(_._2)) {
            writeSuccessFlag(spark.sparkContext.hadoopConfiguration, params.output_base_path)
        } else {
            val errorInputPaths = results.filter(!_._2).map(_._1.xmlInputPath)
            val errorMessage = s"Error(s) occurred processing ${errorInputPaths.mkString(", ")}"
            log.error(errorMessage)
            // Force job failure in case of error in job(s)
            throw new RuntimeException(errorMessage)
        }

    }


}