package org.wikimedia.analytics.refinery.job.druid

import com.netaporter.uri.Uri
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.wikimedia.analytics.refinery.core.Webrequest
import scopt.OptionParser

/**
  *
  * To be launched with Spark 2:
  *

spark-submit --master yarn --deploy-mode cluster \
  --class org.wikimedia.analytics.refinery.job.druid.BannerImpressionsStream \
  --driver-memory 2G --num-executors 4 --executor-cores 3 --executor-memory 4G \
  /home/joal/code/refinery-job-0.0.52-streams.jar
  
  *
  */

object BannerImpressionsStream {

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  def run(
           @transient sc: SparkContext,
           kafkaBrokers:String,
           kafkaInputTopics: String,
           batchDurationSeconds: Int,
           checkpointDirectory: String,
           noCheckpoint: Boolean,
           tranquilityBeamConf: TranquilityBeamConf
         ): Unit = {

    def newStreamingContext() = {
      val ssc = new StreamingContext(sc, Seconds(batchDurationSeconds.toLong))
      ssc.checkpoint(checkpointDirectory)

      val kafkaInputTopicsSet = kafkaInputTopics.split(",").toSet
      val KafkaInputParameters = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

      // Get kafka batches from input topics
      val messageStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        KafkaInputParameters,
        kafkaInputTopicsSet
      )

      // To parse JSON object
      @transient
      implicit lazy val formats = DefaultFormats

      // Compute banner oriented filtering / conversion / aggregation
      val bannerStream: DStream[Map[String, Any]] = messageStream.
        // Extract the JSON message from the Kafka (Key, Value) message.
        flatMap { case (_, str) =>
          try {
            Seq(parse(str))
          } catch {
            case e: Throwable =>
              log.warn("Exception occured when parsing JSON:" + e.getMessage)
              Seq.empty
          }
        }.
        filter(json => {
          try {
            (json \ "uri_path").values.asInstanceOf[String] == "/beacon/impression" &&
              (json \ "uri_query").values.asInstanceOf[String].contains("debug=false") &&
              !Webrequest.getInstance().isSpider((json \ "user_agent").values.asInstanceOf[String])
          } catch {
            case e: Throwable =>
              log.warn("Exception occured when filtering banner related events: " + e.getMessage)
              false
          }
        }).
        flatMap(json => {
          try {
            val uri_query = (json \\ "uri_query").values.asInstanceOf[String]
            val uri: Uri = Uri.parse("http://bla.org/woo/" + uri_query)
            val minuteTs = (json \ "dt").values.asInstanceOf[String].replaceAll(":\\d\\d$", ":00.000Z")
            val params: Map[String, Seq[String]] = uri.query.paramMap

            val mapEvent = Map(
              "timestamp" -> minuteTs,
              "campaign" -> params.getOrElse("campaign", List.empty[String]).headOption,
              "banner" -> params.getOrElse("banner", List.empty[String]).headOption,
              "project" -> params.getOrElse("project", List.empty[String]).headOption,
              "uselang" -> params.getOrElse("uselang", List.empty[String]).headOption,
              "bucket" -> params.getOrElse("bucket", List.empty[String]).headOption,
              "anonymous" -> params.getOrElse("anonymous", List.empty[String]).headOption.contains("true"),
              "status_code" -> params.getOrElse("statusCode", List.empty[String]).headOption,
              "country" -> params.getOrElse("country", List.empty[String]).headOption,
              "device" -> params.getOrElse("device", List.empty[String]).headOption,
              "sample_rate" -> params.getOrElse("recordImpressionSampleRate", List.empty[String]).headOption.map(_.toDouble))
            Seq(mapEvent)
          } catch {
            case e: Throwable =>
              log.warn("Exception occured when building banner json event: " + e.getMessage)
              Seq.empty
          }
        }).
        countByValue().
        map { case (mapEvent, count) =>
          val normalizedRequestCount = mapEvent("sample_rate").asInstanceOf[Option[Double]].map(count / _)
          mapEvent + ("request_count" -> count) + ("normalized_request_count" -> normalizedRequestCount)
        }

      // Add this import to your Spark job to be able to propagate events from any RDD to Druid
      import com.metamx.tranquility.spark.BeamRDD._

      // Output banners to druid through tranquility
      bannerStream.foreachRDD(_.propagate(new TranquilitySingletonBeamFactory(tranquilityBeamConf)))

      ssc
    }

    val context = {
      if (noCheckpoint) newStreamingContext()
      else StreamingContext.getOrCreate(checkpointDirectory, newStreamingContext)
    }

    // Start the context
    context.start()
    context.awaitTermination()
  }

  /**
    * Config class for CLI argument parser using scopt
    */
  case class Params(
                     kafkaBrokers: String = Seq("12", "13", "14", "18", "20", "22").map("kafka10" + _ + ".eqiad.wmnet:9092").mkString(","),
                     kafkaInputTopics: String = "webrequest_text",
                     batchDurationSecs: Int = 30,
                     checkpointDirectory: String = "hdfs://analytics-hadoop/tmp/spark/banner_impressions_stream_checkpoint",
                     noCheckpoint: Boolean = false,
                     zookeeperHosts: String = "druid1001.eqiad.wmnet,druid1002.eqiad.wmnet,druid1003.eqiad.wmnet",
                     zookeeperDruidDiscoveryPath: String = "/druid/analytics-eqiad/discovery",
                     zookeeperDruidIndexingService: String = "druid/overlord",
                     druidDatasource: String = "banner_activity_minutely",
                     indexingSegmentGranularity: DruidGranularityWrapper = DruidGranularityWrapper("DAY"),
                     indexingWindowPeriod: Period = new Period("PT10M"),
                     indexingTaskReplication: Int = 3,
                     indexingTaskPartitions: Int = 1
                   )

  /**
    * Define the command line options parser
    */
  val argsParser = new OptionParser[Params]("Banner Impressions Stream") {
    head("Banner Impressions Stream", "")
    note( "Extract banner impressions data from kafka webrequest stream and write it back to kafka")
    help("help") text "Prints this usage text"

    opt[String]("kafka-brokers") optional() valueName "<broker1,...,brokerN>" action {
      (x, p) => p.copy(kafkaBrokers = x)
    } text "Kafka brokers to consume from. Defaults to kafka10[12|14|18|20|22].eqiad.wmnet:9092"

    opt[String]("kafka-input-topics") optional() valueName "<topic1,...,topicK>" action {
      (x, p) => p.copy(kafkaInputTopics = x)
    } text "Input topics to consume. Defaults to webrequest_text"

    opt[Int]("batch-duration-seconds") optional() action {
      (x, p) => p.copy(batchDurationSecs = x)
    } text "Spark batch duration in seconds. Defaults to 30."

    opt[String]("checkpoint-dir") optional() valueName "<path>" action {
      (x, p) => p.copy(checkpointDirectory = if (x.endsWith("/")) x else x + "/")
    } text ("Temporary directory for check-pointing streaming job.\n\t" +
      "Defaults to hdfs://analytics-hadoop/tmp/spark/banner_impressions_stream_checkpoint")

    opt[Unit]("no-checkpoint") optional() action {
      (_, p) => p.copy(noCheckpoint = true)
    } text "Force NOT using checkpoint if exists (wipes existing checkpoint directory if any)."

    opt[String]("zookeeper-hosts") optional() valueName "<host1,...,hostN>" action {
      (x, p) => p.copy(zookeeperHosts = x)
    } text "Zookeeper hosts handling druid discovery data. Defaults to druid1001.eqiad.wmnet,druid1002.eqiad.wmnet,druid1003.eqiad.wmnet"

    opt[String]("zookeeper-druid-discovery-path") optional() valueName "<path>" action {
      (x, p) => p.copy(zookeeperDruidDiscoveryPath = x)
    } text "Zookeeper data path to Druid discovery service. Defaults to /druid/analytics-eqiad/discovery"

    opt[String]("zookeeper-druid-indexing-service") optional() valueName "<name>" action {
      (x, p) => p.copy(zookeeperDruidIndexingService = x)
    } text "Zookeeper druid indexing service name. Defaults to druid/overlord"

    opt[String]("druid-datasource") optional() valueName "<source>" action {
      (x, p) => p.copy(druidDatasource = x)
    } text "Druid datasource handling banners data. Defaults to banner_activity_minutely"

    opt[String]("druid-indexing-segment-granularity") optional() valueName "<granularity>" action {
      (x, p) => p.copy(indexingSegmentGranularity = DruidGranularityWrapper(x))
    } validate { x => if (DruidGranularityWrapper(x).isValid) success else failure("Invalid segment granularity")
    } text "Druid indexing segment granularity (druid time-period, can be MINUTE, HOUR, DAY, WEEK...). Defaults to DAY"

    opt[String]("druid-indexing-window-period") optional() valueName "<period>" action {
      (x, p) => p.copy(indexingWindowPeriod = new Period(x))
    } validate { x => try {
        new Period(x)
        success
      } catch {
        case e: java.lang.IllegalArgumentException => failure("Invalid window period")
      }
    } text "Druid indexing window (ISO 8601 time-period, can be PT1H, PT10M for instance). Defaults to PT10M"

    opt[Int]("druid-indexing-partitions") optional() valueName "<num>" action {
      (x, p) => p.copy(indexingTaskPartitions = x)
    } text "Druid indexing partitions (parallelisation level). Defaults to 1"

    opt[Int]("druid-indexing-replication") optional() valueName "<num>" action {
      (x, p) => p.copy(indexingTaskReplication = x)
    } text "Druid indexing replication factor for each partition. Defaults to 3"

  }

  def main(args: Array[String]): Unit = {
    val params = args.headOption match {
      // Case when our job options are given as a single string.  Split them
      // and pass them to argsParser.
      case Some("--options") =>
        argsParser.parse(args(1).split("\\s+"), Params()).getOrElse(sys.exit(1))
      // Else the normal usage, each CLI opts can be parsed as a job option.
      case _ =>
        argsParser.parse(args, Params()).getOrElse(sys.exit(1))
    }

    // Exit non-zero if if any refinements failed.
    apply(params)
  }

  def apply(params: Params): Unit = {
    //Setup logging
    val appLogLevel = Level.INFO
    val allLogLevel = Level.ERROR

    Logger.getRootLogger.setLevel(appLogLevel)
    Logger.getLogger("org.wikimedia").setLevel(appLogLevel)

    Logger.getLogger("akka").setLevel(allLogLevel)
    Logger.getLogger("com.databricks").setLevel(allLogLevel)
    Logger.getLogger("DataNucleus").setLevel(allLogLevel)
    Logger.getLogger("org.apache").setLevel(allLogLevel)
    Logger.getLogger("org.spark-project").setLevel(allLogLevel)

    // Initial setup - Spark, SQLContext
    val conf = new SparkConf()
      .setAppName("BannerImpressionsStream")

      /******** SPARK GLOBAL CONFIG **********/
      // Better serializer than java one
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Increase job parallelity by reducing Spark Delay Scheduling (potentially big performance impact (!)) (Default: 3s)`
      .set("spark.locality.wait", "10")
      // Increase max task failures before failing job (Default: 4)
      .set("spark.task.maxFailures", "8")
      // Prevent killing of stages and corresponding jobs from the Spark UI
      .set("spark.ui.killEnabled", "false")
      // Log Spark Configuration in driver log for troubleshooting
      .set("spark.logConf", "true")

      /******** SPARK STREAMING CONFIG **********/
      // [Optional] Tweak to balance data processing parallelism vs. task scheduling overhead (Default: 200ms)
      .set("spark.streaming.blockInterval", "200")
      // Prevent data loss on driver recovery
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      // Enable backpressure, preventing too much data to be stored internally
      .set("spark.streaming.backpressure.enabled", "true")
      // [Optional] Reduce min rate of PID-based backpressure implementation (Default: 100)
      .set("spark.streaming.backpressure.pid.minRate", "10")
      // spark.streaming.backpressure.initialRate
      .set("spark.streaming.backpressure.initialRate", "30")

      /******** SPARK YARN CONFIG **********/
      // [Optional] Set if --driver-memory < 5GB
      .set("spark.yarn.driver.memoryOverhead", "512")
      // [Optional] Set if --executor-memory < 10GB
      .set("spark.yarn.executor.memoryOverhead", "1024")
      // Increase max application master attempts
      // (needs to be <= yarn.resourcemanager.am.max-attempts in YARN, which defaults to 2)
      // (Default: yarn.resourcemanager.am.max-attempts)
      .set("spark.yarn.maxAppAttempts", "4")
      // Attempt counter considers only the last hour (Default: (none))
      .set("spark.yarn.am.attemptFailuresValidityInterval", "1h")
      // Increase max executor failures (Default: max(numExecutors * 2, 3))
      .set("spark.yarn.max.executor.failures", "32")
      // Executor failure counter considers only the last hour
      .set("spark.yarn.executor.failuresValidityInterval", "1h")

    val sc = new SparkContext(conf)

    // Tranquility settings
    val tranquilityBeamConf = new TranquilityBeamConf(
      params.zookeeperHosts,
      params.zookeeperDruidDiscoveryPath,
      params.zookeeperDruidIndexingService,
      params.druidDatasource,
      (m: Map[String, Any]) => DateTime.parse(m("timestamp").asInstanceOf[String]),
      DruidTimestampSpecWrapper("timestamp", "iso", null),
      IndexedSeq("campaign", "banner", "project", "uselang", "bucket", "anonymous", "status_code", "country", "device", "sample_rate"),
      Seq(LongSum("request_count", "request_count"), DoubleSum("normalized_request_count", "normalized_request_count")),
      DruidQueryGranularityWrapper("MINUTE"),
      params.indexingSegmentGranularity,
      params.indexingWindowPeriod,
      params.indexingTaskReplication,
      params.indexingTaskPartitions
    )

    run(
      sc,
      params.kafkaBrokers,
      params.kafkaInputTopics,
      params.batchDurationSecs,
      params.checkpointDirectory,
      params.noCheckpoint,
      tranquilityBeamConf
    )
  }
}
