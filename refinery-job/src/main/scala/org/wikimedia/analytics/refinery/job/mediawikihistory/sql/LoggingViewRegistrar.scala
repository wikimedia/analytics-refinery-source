package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for logging
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     avro, parquet, json, csv
 */
class LoggingViewRegistrar(
  val spark: SparkSession,
  val statsAccumulator: Option[MapAccumulator[String, Long]],
  val numPartitions: Int,
  val wikiClause: String,
  val readerFormat: String
) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  // View names for not reusable views
  private val actorUnprocessedView = "actor_unprocessed"
  private val commentUnprocessedView = "comment_unprocessed"
  private val loggingUnprocessedView = "logging_unprocessed"

  /**
   * Register the logging view in spark session joining the logging unprocessed table
   * to the actor and comment ones using broadcast join tricks
   */
  def run(
    actorUnprocessedPath : String,
    commentUnprocessedPath: String,
    loggingUnprocessedPath: String
  ): Unit = {

    log.info(s"Registering logging view")

    // Register needed unprocessed-views
    spark.read.format(readerFormat).load(actorUnprocessedPath).createOrReplaceTempView(actorUnprocessedView)
    spark.read.format(readerFormat).load(commentUnprocessedPath).createOrReplaceTempView(commentUnprocessedView)
    spark.read.format(readerFormat).load(loggingUnprocessedPath).createOrReplaceTempView(loggingUnprocessedView)

    // Prepare joining logging to actor using broadcast
    val logActorSplitsSql = SQLHelper.skewSplits(loggingUnprocessedView, "wiki_db, log_actor", wikiClause, 4, 3)
    val logActorSplits = spark.sql(logActorSplitsSql)
      .rdd
      .map(row => ((row.getString(0), row.getLong(1)), row.getInt(2)))
      .collect
      .toMap
    val logActorSplitsMap = spark.sparkContext.broadcast(logActorSplits)
    spark.udf.register(
      "getLogActorSplits",
      (wiki_db: String, actor_id: Long) =>
        logActorSplitsMap.value.getOrElse((wiki_db, actor_id), 1)
    )
    spark.udf.register(
      "getLogActorSplitsList",
      (wiki_db: String, actor_id: Long) => {
        val splits = logActorSplitsMap.value.getOrElse((wiki_db, actor_id), 1)
        (0 until splits).toArray
      }
    )

    // Prepare joining logging to comment using broadcast
    val logCommentSplitsSql = SQLHelper.skewSplits(loggingUnprocessedView, "wiki_db, log_comment_id", wikiClause, 4, 3)
    val logCommentSplits = spark.sql(logCommentSplitsSql)
      .rdd
      .map(row => ((row.getString(0), row.getLong(1)), row.getInt(2)))
      .collect
      .toMap
    val logCommentSplitsMap = spark.sparkContext.broadcast(logCommentSplits)
    spark.udf.register(
      "getLogCommentSplits",
      (wiki_db: String, comment_id: Long) =>
        logCommentSplitsMap.value.getOrElse((wiki_db, comment_id), 1)
    )
    spark.udf.register(
      "getLogCommentSplitsList",
      (wiki_db: String, comment_id: Long) => {
        val splits = logCommentSplitsMap.value.getOrElse((wiki_db, comment_id), 1)
        (0 until splits).toArray
      }
    )

    spark.sql(
      // NOTE: Logging table has duplicated rows (same values except for log_id)
      //       We deduplicate them using group by taking the minimum log_id
      s"""
WITH distinct_filtered_logging AS (
  SELECT DISTINCT
    wiki_db,
    -- We take the oldest possible log_id
    FIRST_VALUE(log_id) OVER w AS log_id,
    log_type,
    log_action,
    log_timestamp,
    -- We take the page_id on the same row as log_id
    FIRST_VALUE(log_page) OVER w as log_page,
    log_title,
    log_namespace,
    log_params,
    log_actor,
    log_comment_id
  FROM $loggingUnprocessedView
  WHERE TRUE
    $wikiClause
    -- Drop log wrong timestamps (none as of 2018-12)
    AND log_timestamp IS NOT NULL
    AND LENGTH(log_timestamp) = 14
    AND SUBSTR(log_timestamp, 0, 4) >= '1990'
    -- Not dropping rows without page-links (can be user-oriented events)
    -- Not dropping rows without user-link
  WINDOW w AS (
    PARTITION BY
      wiki_db,
      log_type,
      log_action,
      log_timestamp,
      log_title,
      log_namespace,
      log_params,
      log_actor,
      log_comment_id
    ORDER BY
      log_id
    )
),

logging_actor_comment_splits AS (
  -- Needed to compute the randomized log_actor/log_comment_id in the select.
  -- Random functions are not (yet?) allowed in joining sections.
  SELECT
    wiki_db,
    log_id,
    log_type,
    log_action,
    log_timestamp,
    log_page,
    log_title,
    log_namespace,
    log_params,
    log_actor,
    -- assign a subgroup from log_id among the actor splits
    CAST(COALESCE(log_id, 0) % getLogActorSplits(wiki_db, log_actor) AS INT) AS log_actor_split,
    log_comment_id,
    -- assign a subgroup from log_id among the comment splits
    CAST(COALESCE(log_id, 0) % getLogCommentSplits(wiki_db, log_comment_id) AS INT) AS log_comment_split
  FROM distinct_filtered_logging
),

actor_split AS (
  SELECT
    wiki_db,
    actor_id,
    actor_user,
    actor_name,
    EXPLODE(getLogActorSplitsList(wiki_db, actor_id)) as actor_split
  FROM $actorUnprocessedView
  WHERE TRUE
    $wikiClause
),

comment_split AS (
  SELECT
    wiki_db,
    comment_id,
    comment_text,
    EXPLODE(getLogCommentSplitsList(wiki_db, comment_id)) as comment_split
  FROM $commentUnprocessedView
  WHERE TRUE
    $wikiClause
)

SELECT
  l.wiki_db as wiki_db,
  log_id,
  log_type,
  log_action,
  log_timestamp,
  actor_user,
  actor_name,
  if(actor_name is null, null, actor_user is null) actor_is_anon,
  log_page,
  log_title,
  log_namespace,
  log_params,
  comment_text
FROM logging_actor_comment_splits l
  LEFT JOIN actor_split a
    ON l.wiki_db = a.wiki_db
      AND l.log_actor = a.actor_id
      AND l.log_actor_split = a.actor_split
  LEFT JOIN comment_split c
      ON l.wiki_db = c.wiki_db
        AND l.log_comment_id = c.comment_id
        AND l.log_comment_split = c.comment_split
    """
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.LOGGING_VIEW)

    log.info(s"Logging view registered")

  }

}
