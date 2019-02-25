package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.job.mediawikihistory.sql.SQLHelper
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}


/**
  * This class defines the functions for the page history reconstruction process.
  * It delegates the reconstruction part of it's process to the
  * [[PageHistoryBuilder]] class.
  *
  * The [[run]] function loads [[PageEvent]] and [[PageState]] RDDs from expected
  * already defined logging and page views using [[PageEventBuilder]] utilities.
  * It then calls [[PageHistoryBuilder.run]] to partition the RDDs and rebuild history.
  *
  * It finally writes the resulting [[PageState]] data in parquet format.
  *
  * Note: You can have errors output as well by providing
  * errorsPath to the [[run]] function.
  */
class PageHistoryRunner(
                         val spark: SparkSession,
                         val statsAccumulator: Option[MapAccumulator[String, Long]],
                         val numPartitions: Int
                       ) extends StatsHelper with Serializable {

  import org.apache.spark.sql.SaveMode
  import org.apache.log4j.Logger
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import org.wikimedia.analytics.refinery.core.TimestampHelpers


  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_LOCALIZED_NAMESPACES = "pageHistory.localizedNamespaces"
  val METRIC_EVENTS_PARSING_OK = "pageHistory.eventsParsing.OK"
  val METRIC_EVENTS_PARSING_KO = "pageHistory.eventsParsing.KO"
  val METRIC_INITIAL_STATES = "pageHistory.initialStates"
  val METRIC_WRITTEN_ROWS = "pageHistory.writtenRows"

  /**
    * Extract and clean [[PageEvent]] and [[PageState]] RDDs from expected already
    * defined logging and user views, then launch the reconstruction and
    * writes the results (and potentially the errors).
    *
    * @param outputPath The path to output the reconstructed page history (parquet files)
    * @param errorsPathOption An optional path to output errors (csv files) if defined
    */
  def run(
           outputPath: String,
           errorsPathOption: Option[String]
  ): Unit = {

    log.info(s"Page history jobs starting")

    //***********************************
    // Prepare page events and states RDDs
    //***********************************

    // Work with 4 times more partitions that expected for file production
    spark.sql("SET spark.sql.shuffle.partitions=" + 4 * numPartitions)

    val namespaces = spark
      .sql(s"SELECT * FROM ${SQLHelper.NAMESPACES_VIEW}")
      .rdd
      .map(r => {
        val wikiDb = r.getString(1)
        addOptionalStat(s"$wikiDb.$METRIC_LOCALIZED_NAMESPACES", 1)
        (
          wikiDb,
          r.getInt(2),
          if (r.isNullAt(3)) "" else r.getString(3),
          if (r.isNullAt(4)) "" else r.getString(4),
          r.getInt(5)
        )
      }).collect()

    val canonicalNamespaceMap = namespaces
      .map(t => (t._1, PageEventBuilder.normalizeTitle(t._3)) -> t._2)
      .toMap
    val localizedNamespaceMap = namespaces
      .map(t => (t._1, PageEventBuilder.normalizeTitle(t._4)) -> t._2)
      .toMap

    val isContentNamespaceMap = namespaces
      .map(t => (t._1, t._2) -> (t._5 == 1))
      .toMap.withDefaultValue(false)

    val pageEventBuilder = new PageEventBuilder(
      canonicalNamespaceMap,
      localizedNamespaceMap,
      isContentNamespaceMap
    )
    val parsedPageEvents = spark.sql(
      // NOTE: The following fields are sanitized according to log_deleted on cloud dbs:
      //  &1: log_action, log_namespace, log_title, log_page
      //  &2: log_comment_id, log_comment
      //  &4: log_user_text, log_actor
      //  log_deleted is not null or 0: log_params
      s"""
  SELECT
    log_type,
    log_action,
    log_page,
    log_timestamp,
    log_user,
    log_user_text,
    log_title,
    log_params,
    log_namespace,
    wiki_db,
    log_id,
    log_comment
  FROM ${SQLHelper.LOGGING_VIEW}
  WHERE ((log_type = 'move')
          OR (log_type = 'delete'
              AND log_action IN ('delete', 'delete_redir', 'restore')
          ))
      """)
      .rdd
      .map(row =>
      {
        val pageEvent = {
          if (row.getString(0) == "move") pageEventBuilder.buildMovePageEvent(row)
          else pageEventBuilder.buildSimplePageEvent(row)
        }
        val metricName = if (pageEvent.parsingErrors.isEmpty) METRIC_EVENTS_PARSING_OK else METRIC_EVENTS_PARSING_KO
        addOptionalStat(s"${pageEvent.wikiDb}.$metricName", 1)
        pageEvent
      })

    val pageEvents = parsedPageEvents.filter(_.parsingErrors.isEmpty).cache()

    val pageStates = spark.sql(
      s"""
  SELECT
    wiki_db,
    page_id,
    page_title,
    page_namespace,
    page_is_redirect,
    page_first_rev_timestamp,
    page_first_rev_user_id,
    page_first_rev_user_text
  FROM ${SQLHelper.PAGE_VIEW}
      """)
      .rdd
      .map(row => {
        val wikiDb = row.getString(0)
        val title = row.getString(2)
        val namespace = row.getInt(3)
        val isContentNamespace = isContentNamespaceMap((wikiDb, namespace))
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_STATES", 1L)
        new PageState(
          // No need to check for null as they're filtered in view
          pageId = Some(row.getLong(1)),
          pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(5)),
          pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(5)),
          titleHistorical = title,
          title = title,
          namespaceHistorical = namespace,
          namespaceIsContentHistorical = isContentNamespace,
          namespace = namespace,
          namespaceIsContent = isContentNamespace,
          isRedirect = Some(row.getBoolean(4)),
          isDeleted = false,
          startTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(5)),
          endTimestamp = None,
          causedByEventType = "create",
          causedByUserId = if (row.isNullAt(6)) None else Some(row.getLong(6)),
          causedByUserText = Option(row.getString(7)),
          wikiDb = wikiDb
        )
      })
      .cache()

    log.info(s"Page history data defined, starting reconstruction")


    //***********************************
    // Reconstruct page history
    //***********************************

    val pageHistoryBuilder = new PageHistoryBuilder(spark, statsAccumulator)
    val (pageHistoryRdd, unmatchedEvents) = pageHistoryBuilder.run(pageEvents, pageStates)

    log.info(s"Page history reconstruction done, writing results, errors and stats")


    //***********************************
    // Write results
    //***********************************

    // Write history
    spark.createDataFrame(pageHistoryRdd.map(state => {
          addOptionalStat(s"${state.wikiDb}.$METRIC_WRITTEN_ROWS", 1)
          state.toRow
        }), PageState.schema)
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
    log.info(s"Page history reconstruction results written")

    //***********************************
    // Optionally Write errors
    //***********************************
    errorsPathOption.foreach(errorsPath => {
      val parsingErrorEvents = parsedPageEvents.filter(_.parsingErrors.nonEmpty)
      val errorDf = spark.createDataFrame(
        parsingErrorEvents.map(e => Row(e.wikiDb, "parsing", e.toString))
          .union(unmatchedEvents.map(e => Row(e.wikiDb, "matching", e.toString))
          ),
        StructType(Seq(
          StructField("wiki_db", StringType, nullable = false),
          StructField("error_type", StringType, nullable = false),
          StructField("event", StringType, nullable = false)
        ))
      )
      errorDf.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .option("sep", "\t")
        .save(errorsPath)
      log.info(s"Page history reconstruction errors written")
    })

    log.info(s"Page history jobs done")
  }

}
