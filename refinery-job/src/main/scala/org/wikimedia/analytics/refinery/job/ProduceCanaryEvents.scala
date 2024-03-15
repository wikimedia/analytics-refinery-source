package org.wikimedia.analytics.refinery.job

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.Try
import com.github.nscala_time.time.Imports._
import org.apache.http.client.config.RequestConfig
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventStream, EventStreamConfig, EventStreamFactory, WikimediaDefaults}
import org.wikimedia.eventutilities.core.http.{BasicHttpClient, BasicHttpResult}
import org.wikimedia.eventutilities.core.json.{JsonLoader, JsonSchemaLoader}
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.monitoring.CanaryEventProducer
import org.wikimedia.utils.http.CustomRoutePlanner

import scala.annotation.tailrec

object ProduceCanaryEvents extends LogHelper with ConfigHelper {
    val httpClientTimeoutInSeconds: Int = 10

    /**
      * Config class for use config files and args.
      */
    case class Config(
        stream_name: String,
        timestamp: DateTime,
        schema_base_uris: Seq[String] = Seq(
            "https://schema.discovery.wmnet/repositories/primary/jsonschema",
            "https://schema.discovery.wmnet/repositories/secondary/jsonschema"
        ),
        event_stream_config_uri: String = "https://meta.wikimedia.org/w/api.php",
        event_service_config_uri: Option[String] = None,
        use_wikimedia_http_client: Boolean = true,
        http_routes: Option[String] = None,
        http_timeout: Option[Int] = None,
        dry_run: Boolean = true
    ) {
        // Call validate now so we can throw at instantiation if this Config is not valid.
        validate()

        /**
          * Empty method to be reused if needed
          */
        private def validate(): Unit = {
        }
    }

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        val default: Config = Config("_EXAMPLE_STREAM_", DateTime.now())

        val propertiesDoc: ListMap[String, String] = ListMap(
            "stream_name" ->"""
                |Only this stream will have canary events produced.
                |""".stripMargin,
            "timestamp" ->"""
                |Timestamp in ISO format for which the canary events will be produced.
                |The meta.dt field will be populated with this value.
                |""".stripMargin,
            "schema_base_uris" -> s"""
                |Event schemas will be loaded from these URIs.
                |Default: ${default.schema_base_uris}
                |""".stripMargin,
            "event_stream_config_uri" -> s"""
                |Event stream config will be loaded from this URI. If the URI
                |contains '/api.php', this will be assumed to be a dynamic MediaWiki
                |EventStreamConfig URI endpoint.
                |endpoint. Default: ${default.event_stream_config_uri}
                |""".stripMargin,
            "event_service_config_uri" -> s"""
                |URI or local path to a YAML or JSON config file that
                |maps event intake service name to an HTTP URI.  This
                |will be used to determine the event intake service URI
                |to which a canary event should be produced.
                |""".stripMargin,
            "use_wikimedia_http_client" ->s"""
                |(deprecated) If true, wikimedia-event-utilities WikimediaDefaults.WIKIMEDIA_HTTP_CLIENT
                |will be used when making http request to get event stream config and to post
                |canary events.  This should always be used in production to properly route
                |to internal production API endpoints.  Default: ${default.use_wikimedia_http_client}
                |""".stripMargin,
            "http_routes" ->s"""
                |Set the list of http routes to use when making http requests.
                |Form is: source1=target1,source2=target2.
                |E.g. meta.wikimedia.org=https://api-ro.discovery.wmnet
                |""".stripMargin,
            "http_timeout" ->s"""
                |Set the timeout in seconds for http requests.
                |Default is 10. Ignored if use_wikimedia_http_client is true.
                |""".stripMargin,
            "dry_run" -> s"""
                |Don't actually produce any canary events, just
                |output the events that would have been produced.
                |The default for this is true, so if you want to
                |actually produce events, make sure you set --dry_run=false.
                |Default: ${default.dry_run}
                |""".stripMargin
        )

        val usage: String =
            """
              |Produces canary events to event intake services.
              |This job produces events for the selected stream's event intake services,
              |using the timestamp provided as meta.dt value, and then exits.
              |Canary events for the stream are created from event schema examples.
              |Canary events will be POSTed to event service URIs determined
              |by mapping the destination_event_service stream config setting
              |to a set of datacenter specific URIs.
              |
              |Example:
              |  # Produce canary events for NavigationTiming stream 2024-03-15T19:05:00
              |  java -cp refinery-job.jar \
              |     org.wikimedia.analytics.refinery.job.ProduceCanaryEvents \
              |     --stream_name navigationtimining --timestamp 2024-03-15T19:05:00
              |"""

        /**
          * Loads Config from args
          */
        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException =>
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
            }
            log.info("Loaded ProduceCanaryEvents config:\n" + prettyPrint(config))
            config
        }
    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        // If log4j is not configured with any appenders,
        // add a ConsoleAppender so logs are printed to the console.
        if (!log.getAllAppenders.hasMoreElements) {
            addConsoleLogAppender()
        }
        val config = Config(args)

        val result = apply(config)

        if (result.isSuccess) {
            log.info(s"Succeeded producing canary events to ${config.stream_name}.")
        } else {
            log.error("Encountered exception in produceCanaryEvents.", result.failed.get)
        }

        sys.exit(if (result.isSuccess) 0 else 1)
    }

    def init(config: Config): (EventStreamFactory, BasicHttpClient) = {
        // Use the WIKIMEDIA_HTTP_CLIENT if in WMF production.
        // This routes e.g. meta.wikimedia.org -> api-ro.wikimedia.org.
        val httpClient = if (config.use_wikimedia_http_client) {
            WikimediaDefaults.WIKIMEDIA_HTTP_CLIENT
        } else {
            val builder = BasicHttpClient.builder()
            config.http_routes
                .map {
                    e => CustomRoutePlanner.createMapFromString(e)
                }
                .foreach(_.forEach((s, t) => builder.addRoute(s, t.toURI)))
            config.http_timeout
                .orElse(Option(httpClientTimeoutInSeconds))
                .map(_ * 1000) // to millis
                .map {
                    timeout => RequestConfig.custom().setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).setSocketTimeout(timeout).build()
                }
                .foreach(builder.httpClientBuilder().setDefaultRequestConfig(_))
            builder.build()
        }

        // ResourceLoader that will be used to get stream config and JSONSchemas.
        val resourceLoader = ResourceLoader.builder()
            .withHttpClient(httpClient)
            .setBaseUrls(ResourceLoader.asURLs(config.schema_base_uris.asJava))
            .build()

        // EventSchemaLoader using ResourceLoader.
        val eventSchemaLoader = EventSchemaLoader.builder()
            .setJsonSchemaLoader(JsonSchemaLoader.build(resourceLoader))
            .build()

        // EventStreamConfig using ResourceLoader
        val eventStreamConfig = {
            val eventStreamConfigBuilder = EventStreamConfig.builder()
                .setEventStreamConfigLoader(config.event_stream_config_uri)
                .setJsonLoader(new JsonLoader(resourceLoader))
            // If event_service_config_uri is defined, call setEventServiceToUriMap
            config.event_service_config_uri.foreach(eventStreamConfigBuilder.setEventServiceToUriMap)
            eventStreamConfigBuilder.build()
        }

        // EventStreamFactory using eventSchemaLoader and eventStreamConfig
        val eventStreamFactory = EventStreamFactory.builder()
            .setEventSchemaLoader(eventSchemaLoader)
            .setEventStreamConfig(eventStreamConfig)
            .build()

        (eventStreamFactory, httpClient)
    }

    /**
      * Produces canary events (or just logs them if config.dry_run).
      * @return
      */
    def apply(config: Config): Try[Unit] = {
        val (eventStreamFactory, httpClient) = init(config)
        val targetEventStream: EventStream = eventStreamFactory.createEventStream(config.stream_name)

        Try(
            if (targetEventStream == null) {
                throw new IllegalArgumentException(s"No event stream match the provided stream_name ${config.stream_name}.")
            } else {
                val canaryEventProducer = new CanaryEventProducer(eventStreamFactory, httpClient)

                // Since we often receive 500 from eventgates, let's mitigate the false alerts with some retries.
                retry(3, "Retrying produceCanaryEvents"){
                    produceCanaryEvents(canaryEventProducer, targetEventStream, config.timestamp, config.dry_run)
                }
            }
        )
    }

    /**
      * Produces canary events for each event stream to the proper event intake service.
      */
    def produceCanaryEvents(
        canaryEventProducer: CanaryEventProducer,
        eventStream: EventStream,
        timestamp: DateTime,
        dryRun: Boolean = false
    ): Boolean = {
        // Map of event service URI -> List of canary events to produce to that event service.
        val uriToCanaryEvents = canaryEventProducer.getCanaryEventsToPostForStreams(Seq(eventStream).asJava, timestamp)

        // Build a description string of the POST requests and events for logging.
        val canaryEventsPostDescription = uriToCanaryEvents.asScala.toList.map({
            case (uri, canaryEvents) =>
                s"POST $uri\n  ${CanaryEventProducer.eventsToArrayNode(canaryEvents)}"
        }).mkString("\n")

        log.info(
            {if (dryRun) "DRY-RUN, would have produced " else "Producing "} +
            s"canary events for stream ${eventStream}:\n" +
            canaryEventsPostDescription + "\n"
        )

        if (!dryRun) {
            val results: mutable.Map[URI, BasicHttpResult] = canaryEventProducer.postEventsToUris(
                uriToCanaryEvents
            ).asScala

            val failures = results.filter({ case (_, httpResult) => !httpResult.getSuccess})
            if (failures.isEmpty) {
                log.info("All canary events successfully produced.");
            } else {
                val failuresDescription = failures.map({
                    case (uri, httpResult) =>
                        s"POST $uri => $httpResult. Response body:\n  ${httpResult.getBodyAsString}"
                }).mkString("\n\n")
                log.error("Some canary events failed to be produced:\n" + failuresDescription);
            }

            failures.isEmpty
        } else {
            true
        }

    }

    @tailrec
    private def retry[T](n: Int, msg: String)(fn: => T): T = {
        try {
            fn
        } catch {
            case e: Throwable =>
                log.info(msg)
                Thread.sleep(5000)  // 5 seconds
                if (n > 1) retry(n - 1, msg)(fn)
                else throw e
        }
    }

}
