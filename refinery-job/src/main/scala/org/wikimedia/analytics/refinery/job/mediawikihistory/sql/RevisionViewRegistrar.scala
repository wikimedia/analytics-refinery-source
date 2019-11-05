package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for revision
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     avro, parquet, json, csv
 */
class RevisionViewRegistrar(
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
  private val revisionUnprocessedView = "revision_unprocessed"

  /**
   * Register the revision view in spark session joining the revision unprocessed table
   * to the actor and comment ones using broadcast join tricks. Also join to change_tags
   * predefined view to get tags.
   */
  def run(
    actorUnprocessedPath : String,
    commentUnprocessedPath: String,
    revisionUnprocessedPath: String
  ): Unit = {

    log.info(s"Registering revision view")

    // Assert that needed change_tags view is already registered
    assert(spark.sqlContext.tableNames().contains(SQLHelper.CHANGE_TAGS_VIEW))

    // Register needed unprocessed-views
    spark.read.format(readerFormat).load(actorUnprocessedPath).createOrReplaceTempView(actorUnprocessedView)
    spark.read.format(readerFormat).load(commentUnprocessedPath).createOrReplaceTempView(commentUnprocessedView)
    spark.read.format(readerFormat).load(revisionUnprocessedPath).createOrReplaceTempView(revisionUnprocessedView)

    // Prepare joining revision to actor using broadcast
    val revActorSplitsSql = SQLHelper.skewSplits(revisionUnprocessedView, "wiki_db, rev_actor", wikiClause, 4, 3)
    val revActorSplits = spark.sql(revActorSplitsSql)
      .rdd
      .map(row => ((row.getString(0), row.getLong(1)), row.getInt(2)))
      .collect
      .toMap
    val revActorSplitsMap = spark.sparkContext.broadcast(revActorSplits)
    spark.udf.register(
      "getRevActorSplits",
      (wiki_db: String, actor_id: Long) =>
        revActorSplitsMap.value.getOrElse((wiki_db, actor_id), 1)
    )
    spark.udf.register(
      "getRevActorSplitsList",
      (wiki_db: String, actor_id: Long) => {
        val splits = revActorSplitsMap.value.getOrElse((wiki_db, actor_id), 1)
        (0 until splits).toArray
      }
    )

    // Prepare joining revision to comment using broadcast
    val revCommentSplitsSql = SQLHelper.skewSplits(revisionUnprocessedView, "wiki_db, rev_comment_id", wikiClause, 4, 3)
    val revCommentSplits = spark.sql(revCommentSplitsSql)
      .rdd
      .map(row => ((row.getString(0), row.getLong(1)), row.getInt(2)))
      .collect
      .toMap
    val revCommentSplitsMap = spark.sparkContext.broadcast(revCommentSplits)
    spark.udf.register(
      "getRevCommentSplits",
      (wiki_db: String, comment_id: Long) =>
        revCommentSplitsMap.value.getOrElse((wiki_db, comment_id), 1)
    )
    spark.udf.register(
      "getRevCommentSplitsList",
      (wiki_db: String, comment_id: Long) => {
        val splits = revCommentSplitsMap.value.getOrElse((wiki_db, comment_id), 1)
        (0 until splits).toArray
      }
    )

    spark.sql(
      // TODO: content model and format are nulled, replace with join to slots if needed
      // NOTE: we LEFT join to actor because we want to keep records where actors have been sanitized out
      //       (eg modules/profile/templates/labs/db/views/maintain-views.yaml)
      s"""
WITH revision_actor_comment_splits AS (
  -- Needed to compute the randomized rev_actor/rev_comment_id in the select.
  -- Random functions are not (yet?) allowed in joining sections.
  SELECT
    wiki_db,
    rev_timestamp,
    rev_page,
    rev_id,
    rev_parent_id,
    rev_minor_edit,
    rev_deleted,
    rev_len,
    rev_sha1,
    rev_actor,
    -- assign a random subgroup among the actor splits determined and broadcast above
    CAST(rand() * getRevActorSplits(wiki_db, rev_actor) AS INT) AS rev_actor_split,
    rev_comment_id,
    -- assign a random subgroup among the comment splits determined and broadcast above
    CAST(rand() * getRevCommentSplits(wiki_db, rev_comment_id) AS INT) AS rev_comment_split
  FROM $revisionUnprocessedView
  WHERE TRUE
    $wikiClause
    -- Drop revisions wrong timestamps (none as of 2018-12)
    AND rev_timestamp IS NOT NULL
    AND LENGTH(rev_timestamp) = 14
    AND SUBSTR(rev_timestamp, 0, 4) >= '1990'
    -- Drop revisions with wrong page link
    -- as of 2018-12 happens on 170 wikis with on a relatively small number of revisions
    AND rev_page IS NOT NULL
    AND rev_page > 0
),

actor_split AS (
  SELECT
    wiki_db,
    actor_id,
    actor_user,
    actor_name,
    EXPLODE(getRevActorSplitsList(wiki_db, actor_id)) as actor_split
  FROM $actorUnprocessedView
  WHERE TRUE
    $wikiClause
),

comment_split AS (
  SELECT
    wiki_db,
    comment_id,
    comment_text,
    EXPLODE(getRevCommentSplitsList(wiki_db, comment_id)) as comment_split
  FROM $commentUnprocessedView
  WHERE TRUE
    $wikiClause
)

SELECT
  r.wiki_db AS wiki_db,
  rev_timestamp,
  comment_text,
  actor_user,
  actor_name,
  if(actor_name is null, null, actor_user is null) actor_is_anon,
  rev_page,
  rev_id,
  rev_parent_id,
  rev_minor_edit,
  rev_deleted,
  rev_len,
  rev_sha1,
  null rev_content_model,
  null rev_content_format,
  change_tags AS rev_tags

FROM revision_actor_comment_splits r
  LEFT JOIN actor_split a
    ON r.wiki_db = a.wiki_db
      AND r.rev_actor = a.actor_id
      AND r.rev_actor_split = a.actor_split
  LEFT JOIN comment_split c
    ON r.wiki_db = c.wiki_db
      AND r.rev_comment_id = c.comment_id
      AND r.rev_comment_split = c.comment_split
  LEFT JOIN ${SQLHelper.CHANGE_TAGS_VIEW} ct
    ON r.wiki_db = ct.wiki_db
      AND r.rev_id = ct.ct_rev_id

    """
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.REVISION_VIEW)

    log.info(s"Revision view registered")

  }

}
