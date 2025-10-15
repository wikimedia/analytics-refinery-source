package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.sql.MediawikiMultiContentRevisionSha1
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for archive (revisions)
 *
 * @param spark            the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions    the number of partitions to use for the registered view
 * @param wikiClause       the SQL wiki restriction clause. Should be a valid SQL
 *                         boolean clause based on wiki_db field
 * @param readerFormat     The spark reader format to use. Should be one of
 *                         avro, parquet, json, csv
 */
class ArchiveViewRegistrar(
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
    private val archiveUnprocessedView = "archive_unprocessed"
    private val revisionUnprocessedView = "revision_unprocessed"

    /**
     * Register the archive view in spark session joining the archive unprocessed table
     * to the actor one for user resolution using the broadcast join trick. Also joins
     * to change_tag and change_tag_def to get tags, and finally anti-join to revision
     * to enforce non-duplicated revision_id.
     */
    def run(
        actorUnprocessedPath: String,
        archiveUnprocessedPath: String,
        revisionUnprocessedPath: String
    ): Unit = {

        log.info(s"Registering Archive view")

        // Assert that needed change_tags and slots views are already registered
        assert(spark.sqlContext.tableNames().contains(SQLHelper.CHANGE_TAGS_VIEW))
        assert(spark.sqlContext.tableNames().contains(SQLHelper.SLOTS_VIEW))

        // Register needed unprocessed-views
        spark.read.format(readerFormat).load(actorUnprocessedPath).createOrReplaceTempView(actorUnprocessedView)
        spark.read.format(readerFormat).load(archiveUnprocessedPath).createOrReplaceTempView(archiveUnprocessedView)
        spark.read.format(readerFormat).load(revisionUnprocessedPath).createOrReplaceTempView(revisionUnprocessedView)

        // Prepare UDF to compute revision sha1 from multi-content sha1s
        SQLHelper.registerComputeSha1UDF(spark)

        // Prepare joining archive to actor using broadcast
        val arActorSplitsSql = SQLHelper.skewSplits(archiveUnprocessedView, "wiki_db, ar_actor", wikiClause, 4, 3)
        val arActorSplits = spark.sql(arActorSplitsSql)
            .rdd
            .map(row => ((row.getString(0), row.getLong(1)), row.getInt(2)))
            .collect
            .toMap
        val arActorSplitsMap = spark.sparkContext.broadcast(arActorSplits)

        spark.udf.register(
            "getArActorSplits",
            (wiki_db: String, actor_id: Long) =>
                arActorSplitsMap.value.getOrElse((wiki_db, actor_id), 1)
        )
        spark.udf.register(
            "getArActorSplitsList",
            (wiki_db: String, actor_id: Long) => {
                val splits = arActorSplitsMap.value.getOrElse((wiki_db, actor_id), 1)
                (0 until splits).toArray
            }
        )

        // Register complex view
        spark.sql(
            // TODO: simplify or remove joins as source table imports change
            // TODO: content model and format are nulled, replace with join to slots if needed
            // NOTE: ar_comment_id is always null on labsdb, so we don't process it at all
            // NOTE: ar_len is nulled if ar_deleted&1, not sure how this affects metrics
            // NOTE: ar_actor is 0 on cloud dbs if ar_deleted&4
            // NOTE: ar_sha1 is null on cloud dbs if ar_deleted&1
            s"""
WITH archive_actor_split AS (
  SELECT
    wiki_db,
    ar_timestamp,
    ar_page_id,
    ar_namespace,
    ar_title,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_deleted,
    ar_len,
    ar_actor,
    -- assign a subgroup from ar_rev_id among the actor splits
    CAST(COALESCE(ar_rev_id, 0) % getArActorSplits(wiki_db, ar_actor) AS INT) AS ar_actor_split
  FROM $archiveUnprocessedView
  WHERE TRUE
    $wikiClause
    -- Drop wrong timestamps (none as of 2018-12)
    AND ar_timestamp IS NOT NULL
    AND LENGTH(ar_timestamp) = 14
    AND SUBSTR(ar_timestamp, 0, 4) >= '1990'
    -- Drop wrong page link (no page_id nor page_title)
   AND (((ar_page_id IS NOT NULL) AND (ar_page_id > 0))
     OR ((ar_title IS NOT NULL) AND (length(ar_title) > 0)))
),

actor_split AS (
  SELECT
    wiki_db,
    actor_id,
    actor_user,
    actor_name,
    EXPLODE(getArActorSplitsList(wiki_db, actor_id)) as actor_split
  FROM $actorUnprocessedView
  WHERE TRUE
    $wikiClause
),

revision_reduced AS (
  SELECT
    wiki_db,
    rev_id
  FROM $revisionUnprocessedView
  WHERE TRUE
    $wikiClause
)

SELECT
  ar.wiki_db AS wiki_db,
  ar_timestamp,
  null comment_text,
  actor_user,
  actor_name,
  if(actor_name is null, null, actor_user is null) actor_is_anon,
  ar_page_id,
  ar_title,
  ar_namespace,
  ar_rev_id,
  ar_parent_id,
  ar_minor_edit,
  ar_deleted,
  ar_len,
  CASE WHEN ar_deleted & 1 = 0
    THEN compute_sha1(collect_list(struct(slots.role_name, slots.content_sha1)))
    ELSE NULL
  END ar_sha1,
  NULL ar_content_model,
  NULL ar_content_format,
  change_tags AS ar_tags

FROM archive_actor_split ar
  -- This is needed to prevent archived revisions having
  -- existing live revisions to cause problem
  LEFT ANTI JOIN revision_reduced rev
    ON ar.wiki_db = rev.wiki_db
      AND ar.ar_rev_id = rev.rev_id
  LEFT JOIN actor_split a
    ON ar.wiki_db = a.wiki_db
      AND ar.ar_actor = a.actor_id
      AND ar.ar_actor_split = a.actor_split
  LEFT JOIN ${SQLHelper.CHANGE_TAGS_VIEW} ct
    ON ar.wiki_db = ct.wiki_db
      AND ar.ar_rev_id = ct.ct_rev_id
  LEFT JOIN ${SQLHelper.SLOTS_VIEW} slots
    ON ar.wiki_db = slots.wiki_db
      AND ar.ar_rev_id = slots.slot_revision_id
      AND ar.ar_deleted & 1 = 0

  GROUP BY
    ar.wiki_db,
    ar_timestamp,
    actor_user,
    actor_name,
    ar_page_id,
    ar_title,
    ar_namespace,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_deleted,
    ar_len,
    change_tags

    """
        ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.ARCHIVE_VIEW)

        log.info(s"Archive view registered")

    }

}
