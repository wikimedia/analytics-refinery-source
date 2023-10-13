package org.wikimedia.analytics.refinery.job

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.Try
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
        stream_names: Seq[String] = Seq(),
        settings_filters: Map[String, String] = Map(),
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
          * Validates that configs as provided make sense.
          * Throws IllegalArgumentException if not.
          */
        private def validate(): Unit = {
            if (stream_names.isEmpty && settings_filters.isEmpty) {
                throw new IllegalArgumentException(
                    "Must provide one of --stream_names or --settings_filters to target " +
                    "streams for which to produce canary events."
                )
            }
        }
    }

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        val default: Config = Config(Seq("_EXAMPLE_STREAM_"))

        val propertiesDoc: ListMap[String, String] = ListMap(
            "stream_names" ->"""
                |If provided, only these streams will have canary events produced.
                |Must provide at least one of --stream_names or --settings_filters.
                |""".stripMargin,
            "settings_filters" -> """
                |If provided, only streams that have these settings in Event Stream Config
                |will have canary events produced.
                |Must provide at least one of --stream_names or --settings_filters.
                |Should be provided as settingA:valueA,settingB:valueB
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
            "use_wikimedia_http_client" ->
                s"""(deprecated) If true, wikimedia-event-utilities WikimediaDefaults.WIKIMEDIA_HTTP_CLIENT
               |will be used when making http request to get event stream config and to post
               |canary events.  This should always be used in production to properly route
               |to internal production API endpoints.  Default: ${default.use_wikimedia_http_client}
               |""".stripMargin,
            "http_routes" ->
                s"""Set the list of http routes to use when making http requests.
                   |Form is: source1=target1,source2=target2.
                   |E.g. meta.wikimedia.org=https://api-ro.discovery.wmnet
                   |""".stripMargin,
            "http_timeout" ->
                s"""Set the timeout in seconds for http requests.
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
              |This job produces events for the selected streams and their
              |corresponding event intake services, and then exits.
              |Streams are discovered using the MediaWiki EventStreamConfig API,
              |and canary events for those streams are created from
              |event schema examples.
              |Canary events will be POSTed to event service URIs determined
              |by mapping the destination_event_service stream config setting
              |to a set of datacenter specific URIs.
              |
              |Example:
              |  # Produce canary events for all streams that have canary_events_enabled
              |  # setting set to true in stream config.
              |  java -cp refinery-job.jar \
              |     org.wikimedia.analytics.refinery.job.ProduceCanaryEvents \
              |     --settings_filters=canary_events_enabled:true
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

        val success = apply(config)
        sys.exit(if (success) 0 else 1)
    }

    /**
      * Produces canary events (or just logs them if config.dry_run).
      * @return
      */
    def apply(config: Config): Boolean = {
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

        val targetEventStreams = getTargetEventStreams(
            eventStreamFactory,
            config.stream_names,
            config.settings_filters
        )

        if (targetEventStreams.isEmpty) {
            log.warn(
                "No event streams match the provided stream_names and settings_filters. " +
                "No canary events will be produced."
            )
            false
        } else {
            val canaryEventProducer = new CanaryEventProducer(eventStreamFactory, httpClient)

            // Instead of producing all streams at once, produce them one at a time.
            // This is less efficient (results in an HTTP post for every stream (actually 2,
            // one for each DC)), but allows us to handle errors for each stream separately.
            // See: https://phabricator.wikimedia.org/T270138
            val results: Seq[Try[Boolean]] = targetEventStreams.map { eventStream =>
                Try(
                    // Since we often receive 500 from eventgates, let's mitigate the false alerts with some retries.
                    retry(3, "Retrying produceCanaryEvents"){
                        produceCanaryEvents(canaryEventProducer, Seq(eventStream), config.dry_run)
                    }
                )
            }

            // Log any unexpected exceptions after we are done producing events.
            val exceptions = results.filter(_.isFailure).map(_.failed.get)
            exceptions.foreach(log.error("Encountered exception in produceCanaryEvents.", _))

            // Failures have already been logged.
            val failures = results.filter(r => r.isSuccess && !r.get)
            val successes = results.filter(r => r.isSuccess && r.get)

            if (successes.nonEmpty) {
                log.info(
                    "Succeeded producing canary events to " +
                    s" ${successes.length} / ${targetEventStreams.length} streams."
                )
            }

            if (exceptions.nonEmpty) {
                log.error(
                    "Encountered unexpected exceptions when producing canary events to " +
                    s" ${exceptions.length} / ${targetEventStreams.length} streams."
                )
            }

            if (failures.nonEmpty) {
                log.error(
                    "Encountered failures when producing canary events to " +
                    s"${failures.length} / ${targetEventStreams.length} streams."
                )
            }

            // Return true if there were no unexpected exceptions or failures
            exceptions.isEmpty && failures.isEmpty
        }

    }

    /**
      * Uses eventStreamFactory to get EventStreams for streamNames that have stream configs
      * with settings that match what is provided in settingsFilters.
      * If neither streamNames or settingsFilters are provided, all known EventStreams
      * will be returned.
      *
      * Since settingsFilters must all be strings, this only allows filtering
      * on string stream config settings, or at least ones for which JsonNode.asText()
      * returns something sane (which is true for most primitive types).
      */
    def getTargetEventStreams(
        eventStreamFactory: EventStreamFactory,
        streamNames: Seq[String] = Seq(),
        settingsFilters: Map[String, String] = Map()
    ): Seq[EventStream] = {
        // Instantiate event streams either by stream names, or get them all.
        val eventStreams: Seq[EventStream] = { streamNames match {
            case Seq() => eventStreamFactory.createAllCachedEventStreams
            case _ => eventStreamFactory.createEventStreams(streamNames.asJava)
        }}.asScala.toSeq

        // If settingsFilters are provided, filter for eventStreams that have those settings.
        // This will just return eventStreams as is if settingsFilters is an empty Map.
        eventStreams.filter(eventStream => {
            settingsFilters.forall({ case (settingKey, filterSetting) =>
                val streamSetting = eventStream.getSetting(settingKey)
                streamSetting != null && filterSetting == streamSetting.asText()
            })
        })
    }

    /**
      * Produces canary events for each event stream to the proper event intake service.
      */
    def produceCanaryEvents(
        canaryEventProducer: CanaryEventProducer,
        eventStreams: Seq[EventStream],
        dryRun: Boolean = false
    ): Boolean = {
        // Map of event service URI -> List of canary events to produce to that event service.
        val uriToCanaryEvents = canaryEventProducer.getCanaryEventsToPostForStreams(eventStreams.asJava)

        // Build a description string of the POST requests and events for logging.
        val canaryEventsPostDescription = uriToCanaryEvents.asScala.toList.map({
            case (uri, canaryEvents) =>
                s"POST $uri\n  ${CanaryEventProducer.eventsToArrayNode(canaryEvents)}"
        }).mkString("\n")

        log.info(
            {if (dryRun) "DRY-RUN, would have produced " else "Producing "} +
            "canary events for streams:\n  " +
            eventStreams.map(_.streamName).mkString("\n  ") + "\n" +
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
