package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.wikimedia.analytics.refinery.job.mediawikihistory.DeequColumnAnalysis
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wikimedia.analytics.refinery.tools.LogHelper


/**
 * Class checking a mediawiki-denormalized-history snapshot versus a previously generated one (expected correct).
 * Used by [[org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryChecker]]
 */
class DenormalizedHistoryChecker(
  val spark: SparkSession,
  val mediawikiHistoryBasePath: String,
  val previousSnapshot: String,
  val newSnapshot: String,
  val wikisToCheck: Int,
  val minEventsGrowthThreshold: Double,
  val maxEventsGrowthThreshold: Double,
  val wrongRowsRatioThreshold: Double
) extends LogHelper with Serializable with DeequColumnAnalysis {

  /**
   * Path instantiation at creation
   */
  val outputPath = s"$mediawikiHistoryBasePath/history_check_errors/snapshot=$newSnapshot"
  private val previousDenormHistoryPath = s"$mediawikiHistoryBasePath/history/snapshot=$previousSnapshot"
  private val newDenormHistoryPath = s"$mediawikiHistoryBasePath/history/snapshot=$newSnapshot"

  /**
   * Denormalized metrics for a snapshot (works for both previous and new)
   */
  def getDenormMetrics(denormSnapshot: DataFrame, snapshot: String): DataFrame = {
    val tmpTable = "tmp_denorm"
    denormSnapshot.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db AS wiki_db,
         |    event_entity as event_entity,
         |    event_type AS event_type,
         |    COUNT(1) AS count_denorm_event,
         |    -- User values
         |    COUNT(DISTINCT event_user_id) AS distinct_user_id,
         |    COUNT(DISTINCT event_user_text_historical) AS distinct_user_text,
         |    SUM(IF(ARRAY_CONTAINS(event_user_groups_historical, "bot"), 1, 0)) AS count_user_group_bot,
         |    SUM(IF(event_user_is_anonymous, 1, 0)) AS count_user_anonymous,
         |    SUM(IF(event_user_is_temporary, 1, 0)) AS count_user_temporary,
         |    SUM(IF(event_user_is_permanent, 1, 0)) AS count_user_permanent,
         |    SUM(IF(event_user_is_created_by_self, 1, 0)) AS count_user_self_created,
         |    -- Page values
         |    COUNT(DISTINCT page_id) AS distinct_page_id,
         |    COUNT(DISTINCT page_title_historical) AS distinct_page_title,
         |    COUNT(DISTINCT page_namespace_historical) AS distinct_page_namespace,
         |    SUM(IF(page_is_redirect, 1, 0)) AS count_page_redirect,
         |    -- Revision values
         |    SUM(IF(revision_is_deleted_by_page_deletion, 1, 0)) AS count_revision_deleted,
         |    SUM(IF(revision_is_identity_reverted, 1, 0)) AS count_revision_reverted,
         |    SUM(IF(revision_is_identity_revert, 1, 0)) AS count_revision_revert
         |FROM $tmpTable
         |-- Null event_timestamp means beginning of time, therefore before snapshot :)
         |WHERE (event_timestamp IS NULL OR SUBSTR(event_timestamp, 0, 7) <= '$snapshot')
         |GROUP BY
         |    wiki_db,
         |    event_entity,
         |    event_type
      """.stripMargin)
  }


  def getWikisToCheck(denormMetrics: DataFrame): Seq[String] = {
    val tmpTable = "tmp_denorm_metrics"
    denormMetrics.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db,
         |    SUM(count_denorm_event) AS count_denorm_event
         |FROM $tmpTable
         |GROUP BY
         |    wiki_db
         |ORDER BY count_denorm_event DESC
         |LIMIT $wikisToCheck
      """.stripMargin).collect()
      .map(_.getString(0))
  }


  /**
   * Denormalized Metrics Growth from the previous snapshot to the new one
   */
  def getDenormMetricsGrowth(previousDenormMetrics: DataFrame, newDenormMetrics: DataFrame): DataFrame = {
    val tmpPrevTable = "tmp_prev_denorm_metrics"
    val tmpNewTable = "tmp_new_denorm_metrics"
    previousDenormMetrics.createOrReplaceTempView(tmpPrevTable)
    newDenormMetrics.createOrReplaceTempView(tmpNewTable)
    spark.sql(
      s"""
         |SELECT
         |    COALESCE(p.wiki_db, n.wiki_db) AS wiki_db,
         |    COALESCE(p.event_entity, n.event_entity) AS event_entity,
         |    COALESCE(p.event_type, n.event_type) AS event_type,
         |    MAP(
         |        'growth_count_denorm_event',
         |            (COALESCE(n.count_denorm_event, 0) - COALESCE(p.count_denorm_event, 0)) / COALESCE(NULLIF(p.count_denorm_event, 0), 1),
         |
         |        -- User values
         |        'growth_distinct_user_id',
         |            (COALESCE(n.distinct_user_id, 0) - COALESCE(p.distinct_user_id, 0)) / COALESCE(NULLIF(p.distinct_user_id, 0), 1),
         |        'growth_distinct_user_text',
         |            (COALESCE(n.distinct_user_text, 0) - COALESCE(p.distinct_user_text, 0)) / COALESCE(NULLIF(p.distinct_user_text, 0), 1),
         |        'growth_count_user_group_bot',
         |            (COALESCE(n.count_user_group_bot, 0) - COALESCE(p.count_user_group_bot, 0)) / COALESCE(NULLIF(p.count_user_group_bot, 0), 1),
         |        'growth_count_user_anonymous',
         |            (COALESCE(n.count_user_anonymous, 0) - COALESCE(p.count_user_anonymous, 0)) / COALESCE(NULLIF(p.count_user_anonymous, 0), 1),
         |        'growth_count_user_temporary',
         |            (COALESCE(n.count_user_temporary, 0) - COALESCE(p.count_user_temporary, 0)) / COALESCE(NULLIF(p.count_user_temporary, 0), 1),
         |        'growth_count_user_permanent',
         |            (COALESCE(n.count_user_permanent, 0) - COALESCE(p.count_user_permanent, 0)) / COALESCE(NULLIF(p.count_user_permanent, 0), 1),
         |        'growth_count_user_self_created',
         |            (COALESCE(n.count_user_self_created, 0) - COALESCE(p.count_user_self_created, 0)) / COALESCE(NULLIF(p.count_user_self_created, 0), 1),
         |
         |        -- Page values
         |        'growth_distinct_page_id',
         |            (COALESCE(n.distinct_page_id, 0) - COALESCE(p.distinct_page_id, 0)) / COALESCE(NULLIF(p.distinct_page_id, 0), 1),
         |        'growth_distinct_page_title',
         |            (COALESCE(n.distinct_page_title, 0) - COALESCE(p.distinct_page_title, 0)) / COALESCE(NULLIF(p.distinct_page_title, 0), 1),
         |        'growth_distinct_page_namespace',
         |            (COALESCE(n.distinct_page_namespace, 0) - COALESCE(p.distinct_page_namespace, 0)) / COALESCE(NULLIF(p.distinct_page_namespace, 0), 1),
         |        -- Special case for count_page_redirect: Since this value is set from the current state of pages,
         |        -- there is no historical aspect to it, therefore we measure it's variability month to month,
         |        -- not its growth.
         |        'variability_count_page_redirect',
         |            (COALESCE(n.count_page_redirect, 0) - COALESCE(p.count_page_redirect, 0)) / COALESCE(NULLIF(p.count_page_redirect, 0), 1),
         |
         |        -- Revision values
         |        'growth_count_revision_deleted',
         |            (COALESCE(n.count_revision_deleted, 0) - COALESCE(p.count_revision_deleted, 0)) / COALESCE(NULLIF(p.count_revision_deleted, 0), 1),
         |        'growth_count_revision_reverted',
         |            (COALESCE(n.count_revision_reverted, 0) - COALESCE(p.count_revision_reverted, 0)) / COALESCE(NULLIF(p.count_revision_reverted, 0), 1),
         |        'growth_count_revision_revert',
         |            (COALESCE(n.count_revision_revert, 0) - COALESCE(p.count_revision_revert, 0)) / COALESCE(NULLIF(p.count_revision_revert, 0), 1)
         |    ) AS growths
         |FROM $tmpPrevTable p
         |    FULL OUTER JOIN $tmpNewTable n
         |        ON ((p.wiki_db = n.wiki_db)
         |            AND (p.event_entity = n.event_entity)
         |            AND (p.event_type = n.event_type))
      """.stripMargin).cache()
  }

  /**
   * Denormalized Metrics Growth Errors - When outside threshold boundaries
   */
  def getDenormMetricsGrowthErrors(denormMetricsGrowth: DataFrame
  ): DataFrame = {
    val tmpMetricGrowthTable = "tmp_denorm_metrics_growth"
    denormMetricsGrowth.createOrReplaceTempView(tmpMetricGrowthTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db,
         |    event_entity,
         |    event_type,
         |    growths
         |FROM $tmpMetricGrowthTable
         |WHERE growths['growth_count_denorm_event'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_denorm_event'] > $maxEventsGrowthThreshold
         |
         |    -- User values
         |    OR (event_entity = 'user' AND (
         |        growths['growth_distinct_user_id'] < $minEventsGrowthThreshold
         |        OR growths['growth_distinct_user_id'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_distinct_user_text'] < $minEventsGrowthThreshold
         |        OR growths['growth_distinct_user_text'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_count_user_group_bot'] < $minEventsGrowthThreshold
         |        OR growths['growth_count_user_group_bot'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_count_user_anonymous'] < $minEventsGrowthThreshold
         |        OR growths['growth_count_user_anonymous'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_count_user_temporary'] < $minEventsGrowthThreshold
         |        OR growths['growth_count_user_temporary'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_count_user_permanent'] < $minEventsGrowthThreshold
         |        OR growths['growth_count_user_permanent'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_count_user_self_created'] < $minEventsGrowthThreshold
         |        OR growths['growth_count_user_self_created'] > $maxEventsGrowthThreshold
         |    ))
         |
         |    -- Page values
         |    OR (event_entity = 'page'
         |        AND (
         |        growths['growth_distinct_page_id'] < $minEventsGrowthThreshold
         |        OR growths['growth_distinct_page_id'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_distinct_page_title'] < $minEventsGrowthThreshold
         |        OR growths['growth_distinct_page_title'] > $maxEventsGrowthThreshold
         |
         |        OR growths['growth_distinct_page_namespace'] < $minEventsGrowthThreshold
         |        OR growths['growth_distinct_page_namespace'] > $maxEventsGrowthThreshold
         |
         |        -- Since we measure variability, we set the lower accepted threshold limit to
         |        -- -maxEventsGrowthThreshold
         |        OR growths['variability_count_page_redirect'] < -$maxEventsGrowthThreshold
         |        OR growths['variability_count_page_redirect'] > $maxEventsGrowthThreshold
         |    ))
         |
         |    -- Revision values
         |    OR (event_entity = 'revision' AND (
         |        growths['growth_count_revision_deleted'] < $minEventsGrowthThreshold
         |        OR growths['growth_count_revision_deleted'] > $maxEventsGrowthThreshold
         |
         |        -- Data is present but exhibits too much variability to be checked
         |        -- in the way the rest is. Since this data is no used in wikistat2,
         |        -- check is removed.
         |        --OR growths['growth_count_revision_reverted'] < $minEventsGrowthThreshold
         |        --OR growths['growth_count_revision_reverted'] > $maxEventsGrowthThreshold
         |
         |        --OR growths['growth_count_revision_revert'] < $minEventsGrowthThreshold
         |        --OR growths['growth_count_revision_revert'] > $maxEventsGrowthThreshold
         |    ))
      """.stripMargin).cache()
  }

  /**
   *
   * @param denormMetricsGrowth
   * @return Denormalized History growth error ratio
   */
  def getDenormGrowthErrorsRatio(denormMetricsGrowth: DataFrame): Double = {
    val compliancePredicate: String =
      s"""growths['growth_count_denorm_event'] < $minEventsGrowthThreshold
         |OR growths['growth_count_denorm_event'] > $maxEventsGrowthThreshold
         |OR (event_entity = 'user' AND
         |    (growths['growth_distinct_user_id'] < $minEventsGrowthThreshold
         |    OR growths['growth_distinct_user_id'] > $maxEventsGrowthThreshold
         |    OR growths['growth_distinct_user_text'] < $minEventsGrowthThreshold
         |    OR growths['growth_distinct_user_text'] > $maxEventsGrowthThreshold
         |    OR growths['growth_count_user_group_bot'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_user_group_bot'] > $maxEventsGrowthThreshold
         |    OR growths['growth_count_user_anonymous'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_user_anonymous'] > $maxEventsGrowthThreshold
         |    OR growths['growth_count_user_temporary'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_user_temporary'] > $maxEventsGrowthThreshold
         |    OR growths['growth_count_user_permanent'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_user_permanent'] > $maxEventsGrowthThreshold
         |    OR growths['growth_count_user_self_created'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_user_self_created'] > $maxEventsGrowthThreshold))
         |OR (event_entity = 'page' AND
         |    (growths['growth_distinct_page_id'] < $minEventsGrowthThreshold
         |    OR growths['growth_distinct_page_id'] > $maxEventsGrowthThreshold
         |    OR growths['growth_distinct_page_title'] < $minEventsGrowthThreshold
         |    OR growths['growth_distinct_page_title'] > $maxEventsGrowthThreshold
         |    OR growths['growth_distinct_page_namespace'] < $minEventsGrowthThreshold
         |    OR growths['growth_distinct_page_namespace'] > $maxEventsGrowthThreshold
         |    OR growths['variability_count_page_redirect'] < -$maxEventsGrowthThreshold
         |    OR growths['variability_count_page_redirect'] > $maxEventsGrowthThreshold))
         |OR (event_entity = 'revision' AND
         |    (growths['growth_count_revision_deleted'] < $minEventsGrowthThreshold
         |    OR growths['growth_count_revision_deleted'] > $maxEventsGrowthThreshold))""".stripMargin.replaceAll("\n", " ")

    columnComplianceAnalysis(denormMetricsGrowth, compliancePredicate, "Check Denorm ErrorRatio Metric")
  }

  def checkDenormHistory(): Unit = {
    val previousDenormHistory = spark.read.parquet(previousDenormHistoryPath)
    val newDenormHistory = spark.read.parquet(newDenormHistoryPath)

    val previousDenormMetrics = getDenormMetrics(previousDenormHistory, previousSnapshot)
    val newDenormMetrics = getDenormMetrics(newDenormHistory, newSnapshot)

    val wikisToCheck = getWikisToCheck(newDenormMetrics)

    val denormMetricsGrowth = getDenormMetricsGrowth(
      previousDenormMetrics.where(col("wiki_db").isin(wikisToCheck:_*)),
      newDenormMetrics.where(col("wiki_db").isin(wikisToCheck:_*))
    )

    val errorRowsRatio = getDenormGrowthErrorsRatio(denormMetricsGrowth)
    log.info(s"DenormMetricsGrowthErrors ratio: $errorRowsRatio")

    if (errorRowsRatio > wrongRowsRatioThreshold) {
      log.warn(s"DenormMetricsGrowthErrors ratio $errorRowsRatio is higher " +
        s"than expected threshold $wrongRowsRatioThreshold -- Writing errors")
      val denormMetricsGrowthErrors = getDenormMetricsGrowthErrors(denormMetricsGrowth)
      denormMetricsGrowthErrors.repartition(1).write.mode(SaveMode.Append).json(outputPath)
    }

  }

}
