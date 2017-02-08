package org.wikimedia.analytics.refinery.job

import com.netaporter.uri.Uri
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.wikimedia.analytics.refinery.core.Webrequest
import scopt.OptionParser
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import scala.collection.JavaConverters._


object BannerImpressionsStream {

  def run(
           @transient sc: SparkContext,
           kafkaBrokers:String,
           kafkaInputTopics: String,
           kafkaOutputTopic: String,
           batchDurationSeconds: Int,
           checkpointDirectory: String,
           noCheckpoint: Boolean
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

      // Compute banner oriented filtering / conversion / aggregation
      val bannerStream = messageStream.
        // Extract the JSON message from the Kafka (Key, Value) message.
        map { case (_, str) => parse(str) }.
        filter(json => {
          (json \ "uri_path").values.asInstanceOf[String] == "/beacon/impression" &&
            (json \ "uri_query").values.asInstanceOf[String].contains("debug=false") &&
            !Webrequest.getInstance().isSpider((json \ "user_agent").values.asInstanceOf[String])
        }).
        map(json => {
          val uri_query = (json \\ "uri_query").values.asInstanceOf[String]
          val uri: Uri = Uri.parse("http://bla.org/woo/" + uri_query)
          val minuteTs = (json \ "dt").values.asInstanceOf[String].replaceAll(":\\d\\d$", ":00")
          val params: Map[String, Seq[String]] = uri.query.paramMap

          ("dt" -> minuteTs) ~
            ("campaign" -> params.getOrElse("campaign", List.empty[String]).headOption) ~
            ("banner" -> params.getOrElse("banner", List.empty[String]).headOption) ~
            ("project" -> params.getOrElse("project", List.empty[String]).headOption) ~
            ("uselang" -> params.getOrElse("uselang", List.empty[String]).headOption) ~
            ("bucket" -> params.getOrElse("bucket", List.empty[String]).headOption) ~
            ("anonymous" -> (params.getOrElse("anonymous", List.empty[String]).headOption == Some("true"))) ~
            ("status_code" -> params.getOrElse("statusCode", List.empty[String]).headOption) ~
            ("country" -> params.getOrElse("country", List.empty[String]).headOption) ~
            ("device" -> params.getOrElse("device", List.empty[String]).headOption) ~
            ("sample_rate" -> params.getOrElse("recordImpressionSampleRate", List.empty[String]).headOption.map(_.toDouble))
        }).
        countByValue().
        map { case (json, count) =>
          val jsonSampleRate = json \ "sample_rate"
          json merge (
            ("request_count" -> count) ~
              ("normalized_request_count" -> {
                if (jsonSampleRate != JNothing) Some(count / jsonSampleRate.values.asInstanceOf[Double])
                else None
              })
            )
        }.
        map(j => compact(render(j)))

      // Output banners data back to kafka
      bannerStream.foreachRDD(rdd => {
        System.out.println("# events = " + rdd.count())

        rdd.foreachPartition(partition => {
          // Print statements in this section are shown in the executor's stdout logs
          val props = Map[String, String](
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
          ).asInstanceOf[Map[String, Object]]

          val producer = new KafkaProducer[String, String](props.asJava)
          partition.foreach(record => {
            val data = record.toString
            val message = new ProducerRecord[String, String](kafkaOutputTopic, null, data)
            producer.send(message)
          })
          producer.close()
        })
      })
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
                     kafkaOutputTopic: String = "test_banner_impressions_joal",
                     batchDurationSecs: Int = 10,
                     checkpointDirectory: String = "hdfs://analytics-hadoop/tmp/spark/banner_impressions_stream_checkpoint",
                     noCheckpoint: Boolean = false
                   )

  /**
    * Define the command line options parser
    */
  val argsParser = new OptionParser[Params]("Banner Impressions Stream") {
    head("Banner Impressions Stream", "")
    note( "Extract banner impressions data from kafka webrequest stream and write it back to kafka")
    help("help") text "Prints this usage text"

    opt[String]('k', "kafka-brokers") optional() valueName "<broker1,...,brokerN>" action {
      (x, p) => p.copy(kafkaBrokers = x)
    } text "Kafka brokers to consume from. Defaults to kafka10[12|14|18|20|22].eqiad.wmnet:9092"

    opt[String]('i', "kafka-input-topics") optional() valueName "<topic1,...,topicK>" action {
      (x, p) => p.copy(kafkaInputTopics = x)
    } text "Input topics to consume. Defaults to webrequest_text"

    opt[String]('o', "kafka-output-topic") optional() valueName "<topic>" action {
      (x, p) => p.copy(kafkaOutputTopic = x)
    } text "Output topic to write to. Defaults to test_banner_impressions_joal"

    opt[Int]("batch-duration-seconds") optional() action {
      (x, p) => p.copy(batchDurationSecs = x)
    } text "Batch duration in seconds. Defaults to 10."

    opt[String]("checkpoint-dir") optional() valueName "<path>" action {
      (x, p) => p.copy(checkpointDirectory = if (x.endsWith("/")) x else x + "/")
    } text ("Temporary directory for check-pointing streaming job.\n\t" +
      "Defaults to hdfs://analytics-hadoop/tmp/spark/banner_impressions_stream_checkpoint")

    opt[Unit]("no-checkpoint") optional() action {
      (_, p) => p.copy(noCheckpoint = true)
    } text "Force NOT using checkpoint if exists (wipes existing checkpoint directory if any)."

  }

  def main(args: Array[String]) {
    argsParser.parse(args, Params()) match {

      case Some(params) =>
        // Initial setup - Spark, SQLContext
        val conf = new SparkConf().setAppName("BannerImpressionsStream")
        val sc = new SparkContext(conf)
        run(
          sc,
          params.kafkaBrokers,
          params.kafkaInputTopics,
          params.kafkaOutputTopic,
          params.batchDurationSecs,
          params.checkpointDirectory,
          params.noCheckpoint
        )

      case None => sys.exit(1)
    }
  }
}
