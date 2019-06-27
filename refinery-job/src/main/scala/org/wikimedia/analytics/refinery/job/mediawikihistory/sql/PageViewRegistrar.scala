package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for page
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     com.databricks.spark.avro, parquet, json, csv
 */
class PageViewRegistrar(
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
  private val pageUnprocessedView = "page_unprocessed"

  /**
   * Register the page view in spark session joining the page unprocessed table
   * to the revision table for first-revision timestamp
   */
  def run(
    pageUnprocessedPath : String
  ): Unit = {

    log.info(s"Registering page view")

    // Register needed unprocessed-views
    spark.read.format(readerFormat).load(pageUnprocessedPath).createOrReplaceTempView(pageUnprocessedView)

    // Assert that needed revision and archive views are already registered
    assert(spark.sqlContext.tableNames().contains(SQLHelper.REVISION_VIEW))
    assert(spark.sqlContext.tableNames().contains(SQLHelper.ARCHIVE_VIEW))

    // Register the complex view
    spark.sql(s"""

WITH filtered_page AS (
  SELECT
    wiki_db,
    page_id,
    page_title,
    page_namespace,
    page_is_redirect
  FROM $pageUnprocessedView
  WHERE TRUE
    $wikiClause
    -- Drop wrong page_id
    AND page_id IS NOT NULL
    AND page_id > 0
    -- Drop wrong page_title
    AND page_title IS NOT NULL
),

-- Need both live and archive revisions, as live pages can have both
all_revisions AS (
  SELECT
    wiki_db,
    rev_page AS page_id,
    rev_id as page_first_rev_id,
    actor_user AS page_first_rev_user_id,
    actor_is_anon AS page_first_rev_anon_user,
    actor_name AS page_first_rev_user_text,
    rev_timestamp AS page_first_rev_timestamp
  FROM ${SQLHelper.REVISION_VIEW}

  UNION ALL

  SELECT
    wiki_db,
    ar_page_id AS page_id,
    ar_rev_id as page_first_rev_id,
    actor_user AS page_first_rev_user_id,
    actor_is_anon AS page_first_rev_anon_user,
    actor_name AS page_first_rev_user_text,
    ar_timestamp AS page_first_rev_timestamp
  FROM ${SQLHelper.ARCHIVE_VIEW}
  -- Filter undefined rev_ids and page_ids
  WHERE ar_rev_id IS NOT NULL
    AND ar_rev_id > 0
    AND ar_page_id IS NOT NULL
    AND ar_page_id > 0
),

page_first_revision AS (
  SELECT
      wiki_db,
      page_id,
      page_first_rev_user_id,
      page_first_rev_anon_user,
      page_first_rev_user_text,
      page_first_rev_timestamp
  FROM (
    SELECT
      wiki_db,
      page_id,
      page_first_rev_id,
      page_first_rev_user_id,
      page_first_rev_anon_user,
      page_first_rev_user_text,
      page_first_rev_timestamp,
      row_number() OVER (PARTITION BY wiki_db, page_id ORDER BY page_first_rev_timestamp, page_first_rev_id) as row_num
    FROM all_revisions
  ) t
  -- Only keep first row (minimum timestamp, minimum rev_id if same timestamp)
  WHERE row_num = 1
)

SELECT
  p.wiki_db,
  p.page_id,
  p.page_title,
  p.page_namespace,
  p.page_is_redirect,
  fr.page_first_rev_user_id,
  fr.page_first_rev_anon_user,
  fr.page_first_rev_user_text,
  fr.page_first_rev_timestamp
FROM filtered_page p
  INNER JOIN page_first_revision fr
    ON p.wiki_db = fr.wiki_db
      AND p.page_id = fr.page_id
GROUP BY -- Grouping by to enforce expected partitioning
  p.wiki_db,
  p.page_id,
  p.page_title,
  p.page_namespace,
  p.page_is_redirect,
  fr.page_first_rev_user_id,
  fr.page_first_rev_anon_user,
  fr.page_first_rev_user_text,
  fr.page_first_rev_timestamp

    """
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.PAGE_VIEW)


    log.info(s"Page view registered")

  }

}
