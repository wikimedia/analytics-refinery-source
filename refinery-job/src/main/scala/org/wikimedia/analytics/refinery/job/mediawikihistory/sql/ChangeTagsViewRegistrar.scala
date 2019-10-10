package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for changeTags
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     avro, parquet, json, csv
 */
class ChangeTagsViewRegistrar(
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
  private val changeTagUnprocessedView = "change_tag_unprocessed"
  private val changeTagDefUnprocessedView = "change_tag_def_unprocessed"

  /**
   * Register the change_tags view in spark session joining the change_tag unprocessed table
   * to the change_tag_def one
   */
  def run(
    changeTagUnprocessedPath : String,
    changeTagDefUnprocessedPath : String
  ): Unit = {

    log.info(s"Registering change_tags view")

    // Register needed unprocessed-views
    spark.read.format(readerFormat).load(changeTagUnprocessedPath).createOrReplaceTempView(changeTagUnprocessedView)
    spark.read.format(readerFormat).load(changeTagDefUnprocessedPath).createOrReplaceTempView(changeTagDefUnprocessedView)

    // Register needed UDF
    SQLHelper.registerDedupListUDF(spark)

    spark.sql(
      s"""
WITH filtered_ct AS (
  SELECT
    wiki_db,
    ct_rev_id,
    ct_tag_id
  FROM $changeTagUnprocessedView ct
  WHERE TRUE
    $wikiClause
),

change_tag_def_reduced AS (
  SELECT
    wiki_db,
    ctd_id,
    ctd_name
  FROM $changeTagDefUnprocessedView
  WHERE TRUE
    $wikiClause
)

SELECT
  ct.wiki_db,
  ct.ct_rev_id,
  dedup_list(collect_list(ctd_name)) as change_tags
FROM filtered_ct ct
  INNER JOIN change_tag_def_reduced ctd
    ON ct.wiki_db = ctd.wiki_db
      AND ct.ct_tag_id = ctd.ctd_id
GROUP BY
  ct.wiki_db,
  ct.ct_rev_id
"""
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.CHANGE_TAGS_VIEW)

    log.info(s"change_tags view registered")

  }

}
