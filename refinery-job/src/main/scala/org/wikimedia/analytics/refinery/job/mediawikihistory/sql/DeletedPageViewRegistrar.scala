package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for the deleted_page view
 * build on top of archived revisions
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     avro, parquet, json, csv
 */
class DeletedPageViewRegistrar(
  val spark: SparkSession,
  val statsAccumulator: Option[MapAccumulator[String, Long]],
  val numPartitions: Int,
  val wikiClause: String,
  val readerFormat: String
) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  /**
   * Register the deleted_page view in spark session built mostly from archived revisions:
   *  - One deleted_page per page_id -- not considering revisions with empty or 0 page_id
   *  - Most recent title and namespace from archived revisions
   *  - Most ancient archived revisions for first timestamp and user info
   *  - NULL is_redirect (we can't compute that easily)
   *  - Left-anti-join with page on page_id, to not interfere with existing pages
   */
  def run(): Unit = {

    log.info(s"Registering deleted_page view")

    // Assert that needed archive and page views are already registered
    assert(spark.sqlContext.tableNames().contains(SQLHelper.CENTRALAUTH_VIEW))
    assert(spark.sqlContext.tableNames().contains(SQLHelper.ARCHIVE_VIEW))
    assert(spark.sqlContext.tableNames().contains(SQLHelper.PAGE_VIEW))

    // Register the complex view
    spark.sql(s"""
WITH global_user_match AS (
  SELECT
    wiki_db,
    user_id,
    user_central_id
  FROM ${SQLHelper.CENTRALAUTH_VIEW}
  WHERE TRUE
    ${wikiClause}
),
deleted_pageview_prep AS (
  SELECT DISTINCT
    wiki_db,
    ar_page_id as page_id,
    LAST_VALUE(ar_title) OVER by_page_window AS page_title,
    LAST_VALUE(ar_namespace) OVER by_page_window AS page_namespace,
    NULL as page_is_redirect,
    FIRST_VALUE(ar_timestamp) OVER by_page_window AS page_first_rev_timestamp,
    FIRST_VALUE(actor_user) OVER by_page_window AS page_first_rev_user_id,
    FIRST_VALUE(actor_is_anon) OVER by_page_window AS page_first_rev_anon_user,
    FIRST_VALUE(actor_name) OVER by_page_window AS page_first_rev_user_text
  FROM ${SQLHelper.ARCHIVE_VIEW} a
    -- Only keep page_id not present in page
    LEFT ANTI JOIN ${SQLHelper.PAGE_VIEW} p_id
      ON a.wiki_db = p_id.wiki_db
        AND a.ar_page_id = p_id.page_id
    -- Only keep title/ns not present in page
    LEFT ANTI JOIN ${SQLHelper.PAGE_VIEW} p_title_ns
      ON a.wiki_db = p_title_ns.wiki_db
        AND a.ar_title = p_title_ns.page_title
        AND a.ar_namespace = p_title_ns.page_namespace
  WHERE ar_page_id IS NOT NULL
    AND ar_page_id > 0
  WINDOW by_page_window AS (
    PARTITION BY wiki_db, ar_page_id ORDER BY ar_timestamp, ar_rev_id
  )
)
SELECT
    dp.wiki_db,
    dp.page_id,
    dp.page_title,
    dp.page_namespace,
    dp.page_is_redirect,
    dp.page_first_rev_timestamp,
    dp.page_first_rev_user_id,
    gu.user_central_id AS page_first_rev_user_central_id,
    dp.page_first_rev_anon_user,
    dp.page_first_rev_user_text
FROM deleted_pageview_prep dp
LEFT JOIN global_user_match gu
ON dp.wiki_db = gu.wiki_db AND dp.page_first_rev_user_id = gu.user_id
    """
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.DELETED_PAGE_VIEW)

    log.info(s"deleted_page view registered")

  }

}
