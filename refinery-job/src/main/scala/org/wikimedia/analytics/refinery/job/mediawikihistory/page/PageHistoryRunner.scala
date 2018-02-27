package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.StatsHelper


/**
  * This class defines the functions for the page history reconstruction process.
  * It delegates the reconstruction part of it's process to the
  * [[PageHistoryBuilder]] class.
  *
  * The [[run]] function loads [[PageEvent]] and [[PageState]] RDDs from raw path
  * using [[PageEventBuilder]] utilities. It then calls
  * [[PageHistoryBuilder.run]] to partition the RDDs and rebuild history.
  *
  * It finally writes the resulting [[PageState]] data in parquet format.
  *
  * Note: You can have errors output as well by providing
  * errorsPath to the [[run]] function.
  */
class PageHistoryRunner(val spark: SparkSession) extends StatsHelper with Serializable {

  import org.apache.spark.sql.SaveMode
  import com.databricks.spark.avro._
  import org.apache.log4j.Logger
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers


  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_NAMESPACES_COUNT = "pages.namespaces.count"
  val METRIC_MOVE_EVENTS_OK = "pages.moveEvents.OK"
  val METRIC_MOVE_EVENTS_KO = "pages.moveEvents.KO"
  val METRIC_DEL_REST_EVENTS_OK = "pages.delRestEvents.OK"
  val METRIC_DEL_REST_EVENTS_KO = "pages.delRestEvents.KO"
  val METRIC_STATES_COUNT = "pages.states.count"

  /**
    * Extract and clean [[PageEvent]] and [[PageState]] RDDs,
    * then launch the reconstruction and
    * writes the results (and potentially the errors).
    *
    * @param wikiConstraint The wiki database names on which to execute the job (empty for all wikis)
    * @param loggingDataPath The path of the logging data (avro files partitioned by wiki_db)
    * @param pageDataPath The path of the page data (avro files partitioned by wiki_db)
    * @param revisionDataPath The path of the revision data (avro files partitioned by wiki_db)
    * @param namespacesPath The path of the namespaces data (CSV file)
    * @param outputPath The path to output the reconstructed page history (parquet files)
    * @param sqlPartitions The number of partitions to use as a bases for raw RDDs
    * @param errorsPath A path to output errors (csv files)
    * @param statsPath A path to output statistics (csv files)
    */
  def run(
           wikiConstraint: Seq[String],
           loggingDataPath: String,
           pageDataPath: String,
           revisionDataPath: String,
           namespacesPath: String,
           outputPath: String,
           sqlPartitions: Int,
           errorsPath: String,
           statsPath: String
  ): Unit = {

    log.info(s"Page history jobs starting")

    //***********************************
    // Prepare page events and states RDDs
    //***********************************

    spark.sql("SET spark.sql.shuffle.partitions=" + sqlPartitions)

    val loggingDf = spark.read.avro(loggingDataPath)
    loggingDf.createOrReplaceTempView("logging")
    spark.table("logging")

    val pageDf = spark.read.avro(pageDataPath)
    pageDf.createOrReplaceTempView("page")
    spark.table("page")

    val revisionDf = spark.read.avro(revisionDataPath)
    revisionDf.createOrReplaceTempView("revision")
    spark.table("revision")

    val wikiClause = if (wikiConstraint.isEmpty) "" else {
      "AND wiki_db IN (" + wikiConstraint.map(w => s"'$w'").mkString(", ") + ")\n"
    }

    val namespacesCsvSchema = StructType(
        Seq(StructField("domain", StringType, nullable = false),
            StructField("wiki_db", StringType, nullable = false),
            StructField("namespace", IntegerType, nullable = false),
            StructField("namespace_canonical_name",
                        StringType,
                        nullable = false),
            StructField("namespace_localized_name",
                        StringType,
                        nullable = false),
            StructField("is_content", IntegerType, nullable = false)))

    val namespaces = spark.read
      .schema(namespacesCsvSchema)
      .csv(namespacesPath)
      .rdd
      .map(r => {
        val wikiDb = r.getString(1)
        statsAccumulator.add((s"$wikiDb.$METRIC_NAMESPACES_COUNT", 1))
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

    val movePageEventsRdd = spark.sql(
      s"""
  SELECT
    log_type,
    log_timestamp,
    log_user,
    log_title,
    log_params,
    log_namespace,
    wiki_db
  FROM logging
  WHERE log_type = 'move'
      $wikiClause
  GROUP BY -- Grouping by to enforce expected partitioning
    log_type,
    log_timestamp,
    log_user,
    log_title,
    log_params,
    log_namespace,
    wiki_db
      """)
      .rdd
      .map(row => {
        val pageEvent = PageEventBuilder.buildMovePageEvent(
          canonicalNamespaceMap,
          localizedNamespaceMap,
          isContentNamespaceMap)(row)
        val metricName = if (pageEvent.parsingErrors.isEmpty) METRIC_MOVE_EVENTS_OK else METRIC_MOVE_EVENTS_KO
        statsAccumulator.add((s"${pageEvent.wikiDb}.$metricName", 1))
        pageEvent
      })

    val deleteAndRestorePageEventsRdd = spark.sql(
      s"""
  SELECT
    log_page,
    log_title,
    log_namespace,
    log_timestamp as start,
    log_user,
    wiki_db,
    log_action
  FROM logging
  WHERE log_type = 'delete'
    AND log_action IN ('delete', 'restore')
    $wikiClause
  GROUP BY -- Grouping by to enforce expected partitioning
    log_page,
    log_title,
    log_namespace,
    log_timestamp,
    log_user,
    wiki_db,
    log_action
        """)
      .rdd
      .map(
        row => {
          val pageEvent = PageEventBuilder.buildSimplePageEvent(isContentNamespaceMap)(row)
          val metricName = if (pageEvent.parsingErrors.isEmpty) METRIC_DEL_REST_EVENTS_OK else METRIC_DEL_REST_EVENTS_KO
          statsAccumulator.add((s"${pageEvent.wikiDb}.$metricName", 1))
          pageEvent
        })

    // DON'T REPARTITION !!!!!
    // See https://issues.apache.org/jira/browse/SPARK-10685
    val parsedPageEvents = movePageEventsRdd.union(deleteAndRestorePageEventsRdd).cache()
    val pageEvents = parsedPageEvents.filter(_.parsingErrors.isEmpty).cache()

    val pageStates = spark.sql(
      s"""
  SELECT
    page_id,
    rev.rev_timestamp,
    page_title,
    page_namespace,
    rev2.rev_user,
    page.wiki_db,
    page_is_redirect
  FROM page
    INNER JOIN (
      -- crazy but true: there are multiple revisions with rev_parent_id = 0 for the same page
      SELECT
        min(rev_timestamp) as rev_timestamp,
        rev_page,
        wiki_db as wiki_db_rev
      FROM revision
      WHERE TRUE
        $wikiClause
      GROUP BY
        rev_page,
        wiki_db
    ) rev
      ON page_id = rev_page
        AND page.wiki_db = rev.wiki_db_rev
    INNER JOIN (
      SELECT
        rev_page,
        wiki_db as wiki_db_rev2,
        rev_timestamp,
        rev_user
      FROM revision
      WHERE TRUE
        $wikiClause
    ) rev2
      ON rev.rev_page = rev2.rev_page
        AND rev.wiki_db_rev = rev2.wiki_db_rev2
        AND rev.rev_timestamp = rev2.rev_timestamp
  WHERE page.page_title IS NOT NULL -- Used for Graph partitioning, not accepting undefined
    $wikiClause
  GROUP BY -- Grouping by to enforce expected partitioning
    page_id,
    rev.rev_timestamp,
    page_title,
    page_namespace,
    rev2.rev_user,
    page.wiki_db,
    page_is_redirect
      """)
      .rdd
      .map(row => {
        val wikiDb = row.getString(5)
        val title = row.getString(2)
        val namespace = row.getInt(3)
        val isContentNamespace = isContentNamespaceMap((wikiDb, namespace))
        statsAccumulator.add((s"$wikiDb.$METRIC_STATES_COUNT", 1L))
        new PageState(
          pageId = if (row.isNullAt(0)) None else Some(row.getLong(0)),
          pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestamp(row.getString(1)),
          pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestamp(row.getString(1)),
          titleHistorical = title,
          title = title,
          namespaceHistorical = namespace,
          namespaceIsContentHistorical = isContentNamespace,
          namespace = namespace,
          namespaceIsContent = isContentNamespace,
          isRedirect = Some(row.getBoolean(6)),
          startTimestamp = TimestampHelpers.makeMediawikiTimestamp(row.getString(1)),
          endTimestamp = None,
          causedByEventType = "create",
          causedByUserId = if (row.isNullAt(4)) None else Some(row.getLong(4)),
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

    log.info(s"Page history reconstruction done, writing results and errors")


    //***********************************
    // Write results
    //***********************************

    // Write history
    val pageHistoryDf = spark.createDataFrame(pageHistoryRdd.map(_.toRow), PageState.schema)
    //spark.setConf("spark.sql.parquet.compression.codec", "snappy")
    pageHistoryDf.write.mode(SaveMode.Overwrite).parquet(outputPath)
    log.info(s"Page history reconstruction results written")

    //***********************************
    // Write errors
    //***********************************

    val parsingErrorEvents = parsedPageEvents.filter(_.parsingErrors.nonEmpty)
    val errorDf = spark.createDataFrame(
        parsingErrorEvents.map(e => Row(e.wikiDb, "parsing", e.toString)).union(
        unmatchedEvents.map(e => Row(e.wikiDb, "matching", e.toString))
      ),
      StructType(Seq(
        StructField("wiki_db", StringType, nullable = false),
        StructField("error_type", StringType, nullable = false),
        StructField("event", StringType, nullable = false)
      ))
    )
    errorDf.write.mode(SaveMode.Overwrite).format("csv").option("sep", "\t").save(errorsPath)
    log.info(s"Page history reconstruction errors written")


    //***********************************
    // Write stats
    //***********************************
    statsDataframe.write.mode(SaveMode.Overwrite).format("csv").option("sep", "\t").save(statsPath)
    log.info(s"Page history reconstruction stats written")

    log.info(s"Page history jobs done")
  }

}
