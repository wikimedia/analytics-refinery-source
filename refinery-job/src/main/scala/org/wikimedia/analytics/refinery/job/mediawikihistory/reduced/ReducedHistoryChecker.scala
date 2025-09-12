package org.wikimedia.analytics.refinery.job.mediawikihistory.reduced

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wikimedia.analytics.refinery.job.mediawikihistory.{DeequColumnAnalysis, MediawikiHistoryCheckerConfig}
import org.wikimedia.analytics.refinery.tools.LogHelper


/**
 * Class checking a mediawiki-reduced-history snapshot versus a previously generated one (expected correct).
 * Used by [[org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryChecker]]
 */
class ReducedHistoryChecker(
  val spark: SparkSession,
  val mediawikiHistoryBasePath: String,
  val previousSnapshot: String,
  val newSnapshot: String,
  val wikisToCheck: Int,
  val thresholdsConfig: MediawikiHistoryCheckerConfig
) extends LogHelper with Serializable with DeequColumnAnalysis {

  /**
   * Path instantiation at creation
   */
  val outputPath = s"$mediawikiHistoryBasePath/history_reduced_check_errors/snapshot=$newSnapshot"
  private val previousReducedHistoryPath = s"$mediawikiHistoryBasePath/history_reduced/snapshot=$previousSnapshot"
  private val newReducedHistoryPath = s"$mediawikiHistoryBasePath/history_reduced/snapshot=$newSnapshot"

  /**
   * Reduced metrics for a snapshot (works for both previous and new)
   */
  def getReducedMetrics(reducedSnapshot: DataFrame, snapshot: String): DataFrame = {
    val tmpTable = "tmp_reduced"
    reducedSnapshot.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    project AS project,
         |    event_entity AS event_entity,
         |    event_type AS event_type,
         |    COUNT(1) AS count_reduced_event,
         |    -- User values
         |    COUNT(DISTINCT user_text) AS distinct_user_text,
         |    SUM(IF(user_type == 'anonymous', 1, 0)) AS count_user_anonymous,
         |    SUM(IF(user_type == 'group_bot', 1, 0)) AS count_user_group_bot,
         |    SUM(IF(user_type == 'name_bot', 1, 0)) AS count_user_name_bot,
         |    SUM(IF(user_type == 'user', 1, 0)) AS count_user_user,
         |    SUM(IF(ARRAY_CONTAINS(other_tags, 'self_created'), 1, 0)) AS count_user_self_created,
         |    -- Page values
         |    COUNT(DISTINCT page_title) AS distinct_page_title,
         |    COUNT(DISTINCT page_namespace) AS distinct_page_namespace,
         |    SUM(IF(page_type == 'content', 1, 0)) AS count_page_content,
         |    SUM(IF(page_type == 'non_content', 1, 0)) AS count_page_non_content,
         |    SUM(IF(ARRAY_CONTAINS(other_tags, 'redirect'), 1, 0)) AS count_page_redirect,
         |    -- Revision values
         |    SUM(IF(ARRAY_CONTAINS(other_tags, 'deleted'), 1, 0)) AS count_revision_deleted,
         |    SUM(IF(ARRAY_CONTAINS(other_tags, 'reverted'), 1, 0)) AS count_revision_reverted,
         |    SUM(IF(ARRAY_CONTAINS(other_tags, 'revert'), 1, 0)) AS count_revision_revert,
         |    -- Digests values
         |    SUM(revisions) AS count_revisions,
         |    SUM(text_bytes_diff) AS sum_text_bytes_diff,
         |    SUM(text_bytes_diff_abs) AS sum_text_bytes_diff_abs
         |FROM $tmpTable
         |WHERE SUBSTR(event_timestamp, 0, 7) <= '$snapshot'
         |GROUP BY
         |    project,
         |    event_entity,
         |    event_type
      """.stripMargin)
  }


  def getProjectsToCheck(reducedMetrics: DataFrame): Seq[String] = {
    val tmpTable = "tmp_reduced_metrics"
    reducedMetrics.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    project,
         |    SUM(count_reduced_event) AS count_reduced_event
         |FROM $tmpTable
         |GROUP BY
         |    project
         |ORDER BY count_reduced_event DESC
         |LIMIT $wikisToCheck
      """.stripMargin).collect()
      .map(_.getString(0))
  }


  /**
   * Reduced Metrics Growth from the previous snapshot to the new one
   */
  def getReducedMetricsGrowth(previousReducedMetrics: DataFrame, newReducedMetrics: DataFrame): DataFrame = {
    val tmpPrevTable = "tmp_prev_reduced_metrics"
    val tmpNewTable = "tmp_new_reduced_metrics"
    previousReducedMetrics.createOrReplaceTempView(tmpPrevTable)
    newReducedMetrics.createOrReplaceTempView(tmpNewTable)
    spark.sql(
      s"""
         |SELECT
         |    COALESCE(p.project, n.project) AS project,
         |    COALESCE(p.event_entity, n.event_entity) AS event_entity,
         |    COALESCE(p.event_type, n.event_type) AS event_type,
         |    MAP(
         |        'growth_count_reduced_event',
         |            (COALESCE(n.count_reduced_event, 0) - COALESCE(p.count_reduced_event, 0)) / COALESCE(NULLIF(p.count_reduced_event, 0), 1),
         |
         |        -- User values
         |        'growth_distinct_user_text',
         |            (COALESCE(n.distinct_user_text, 0) - COALESCE(p.distinct_user_text, 0)) / COALESCE(NULLIF(p.distinct_user_text, 0), 1),
         |        'growth_count_user_group_bot',
         |            (COALESCE(n.count_user_group_bot, 0) - COALESCE(p.count_user_group_bot, 0)) / COALESCE(NULLIF(p.count_user_group_bot, 0), 1),
         |        'growth_count_user_name_bot',
         |            (COALESCE(n.count_user_name_bot, 0) - COALESCE(p.count_user_name_bot, 0)) / COALESCE(NULLIF(p.count_user_name_bot, 0), 1),
         |        'growth_count_user_anonymous',
         |            (COALESCE(n.count_user_anonymous, 0) - COALESCE(p.count_user_anonymous, 0)) / COALESCE(NULLIF(p.count_user_anonymous, 0), 1),
         |        'growth_count_user_user',
         |            (COALESCE(n.count_user_user, 0) - COALESCE(p.count_user_user, 0)) / COALESCE(NULLIF(p.count_user_user, 0), 1),
         |        'growth_count_user_self_created',
         |            (COALESCE(n.count_user_self_created, 0) - COALESCE(p.count_user_self_created, 0)) / COALESCE(NULLIF(p.count_user_self_created, 0), 1),
         |
         |        -- Page values
         |        'growth_distinct_page_title',
         |            (COALESCE(n.distinct_page_title, 0) - COALESCE(p.distinct_page_title, 0)) / COALESCE(NULLIF(p.distinct_page_title, 0), 1),
         |        'growth_distinct_page_namespace',
         |            (COALESCE(n.distinct_page_namespace, 0) - COALESCE(p.distinct_page_namespace, 0)) / COALESCE(NULLIF(p.distinct_page_namespace, 0), 1),
         |        'growth_count_page_content',
         |            (COALESCE(n.count_page_content, 0) - COALESCE(p.count_page_content, 0)) / COALESCE(NULLIF(p.count_page_content, 0), 1),
         |        'growth_count_page_non_content',
         |            (COALESCE(n.count_page_non_content, 0) - COALESCE(p.count_page_non_content, 0)) / COALESCE(NULLIF(p.count_page_non_content, 0), 1),
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
         |            (COALESCE(n.count_revision_revert, 0) - COALESCE(p.count_revision_revert, 0)) / COALESCE(NULLIF(p.count_revision_revert, 0), 1),
         |
         |        -- Digests values
         |        'growth_count_revisions',
         |            (COALESCE(n.count_revisions, 0) - COALESCE(p.count_revisions, 0)) / COALESCE(NULLIF(p.count_revisions, 0), 1),
         |        -- Same as for page-redirect, we measure variability here as values can be positive or negative
         |        'variability_sum_text_bytes_diff',
         |            (COALESCE(n.sum_text_bytes_diff, 0) - COALESCE(p.sum_text_bytes_diff, 0)) / COALESCE(NULLIF(p.sum_text_bytes_diff, 0), 1),
         |        'growth_sum_text_bytes_diff_abs',
         |            (COALESCE(n.sum_text_bytes_diff_abs, 0) - COALESCE(p.sum_text_bytes_diff_abs, 0)) / COALESCE(NULLIF(p.sum_text_bytes_diff_abs, 0), 1)
         |    ) AS growths
         |FROM $tmpPrevTable p
         |    FULL OUTER JOIN $tmpNewTable n
         |        ON ((p.project = n.project)
         |            AND (p.event_entity = n.event_entity)
         |            AND (p.event_type = n.event_type))
      """.stripMargin).cache()
  }

  /**
   * Reduced Metrics Growth Errors - When outside threshold boundaries
   */
  def getReducedMetricsGrowthErrors(reducedMetricsGrowth: DataFrame
  //  ,pageFalsePositives: DataFrame
  ): DataFrame = {
    val tmpMetricGrowthTable = "tmp_reduced_metrics_growth"
    reducedMetricsGrowth.createOrReplaceTempView(tmpMetricGrowthTable)
    spark.sql(
      s"""
         |SELECT
         |    project,
         |    event_entity,
         |    event_type,
         |    growths
         |FROM $tmpMetricGrowthTable
         |WHERE growths['growth_count_reduced_event'] < ${thresholdsConfig.growth_count_reduced_event_min}
         |    OR growths['growth_count_reduced_event'] > ${thresholdsConfig.growth_count_reduced_event_max}
         |
         |    -- User values with digests
         |    OR (event_entity = 'user' AND (
         |        growths['growth_distinct_user_text'] < ${thresholdsConfig.growth_distinct_user_text_min}
         |        OR growths['growth_distinct_user_text'] > ${thresholdsConfig.growth_distinct_user_text_max}
         |
         |        OR growths['growth_count_user_group_bot'] < ${thresholdsConfig.growth_count_user_group_bot_min}
         |        OR growths['growth_count_user_group_bot'] > ${thresholdsConfig.growth_count_user_group_bot_max}
         |
         |        OR growths['growth_count_user_name_bot'] < ${thresholdsConfig.growth_count_user_name_bot_min}
         |        OR growths['growth_count_user_name_bot'] > ${thresholdsConfig.growth_count_user_name_bot_max}
         |
         |        OR growths['growth_count_user_anonymous'] < ${thresholdsConfig.growth_count_user_anonymous_min}
         |        OR growths['growth_count_user_anonymous'] > ${thresholdsConfig.growth_count_user_anonymous_max}
         |
         |        OR growths['growth_count_user_user'] < ${thresholdsConfig.growth_count_user_user_min}
         |        OR growths['growth_count_user_user'] > ${thresholdsConfig.growth_count_user_user_max}
         |
         |        OR growths['growth_count_user_self_created'] < ${thresholdsConfig.growth_count_user_self_created_min}
         |        OR growths['growth_count_user_self_created'] > ${thresholdsConfig.growth_count_user_self_created_max}
         |
         |        OR growths['growth_count_revisions'] < ${thresholdsConfig.growth_count_revisions_min}
         |        OR growths['growth_count_revisions'] > ${thresholdsConfig.growth_count_revisions_max}
         |
         |        -- Variability not growth --> between -max and max
         |        OR growths['variability_sum_text_bytes_diff'] < -${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |        OR growths['variability_sum_text_bytes_diff'] > ${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |
         |        OR growths['growth_sum_text_bytes_diff_abs'] < ${thresholdsConfig.growth_sum_text_bytes_diff_abs_min}
         |        OR growths['growth_sum_text_bytes_diff_abs'] > ${thresholdsConfig.growth_sum_text_bytes_diff_abs_max}
         |    ))
         |
         |    -- Page values with digests
         |    OR (event_entity = 'page' AND (
         |        growths['growth_distinct_page_title'] < ${thresholdsConfig.growth_distinct_page_title_min}
         |        OR growths['growth_distinct_page_title'] > ${thresholdsConfig.growth_distinct_page_title_max}
         |
         |        OR growths['growth_distinct_page_namespace'] < ${thresholdsConfig.growth_distinct_page_namespace_min}
         |        OR growths['growth_distinct_page_namespace'] > ${thresholdsConfig.growth_distinct_page_namespace_max}
         |
         |        OR growths['growth_count_page_content'] < ${thresholdsConfig.growth_count_page_content_min}
         |        OR growths['growth_count_page_content'] > ${thresholdsConfig.growth_count_page_content_max}
         |
         |        OR growths['growth_count_page_non_content'] < ${thresholdsConfig.growth_count_page_non_content_min}
         |        OR growths['growth_count_page_non_content'] > ${thresholdsConfig.growth_count_page_non_content_max}
         |
         |        -- Since we measure variability, we set the lower accepted threshold limit to
         |        -- -maxEventsGrowthThreshold.
         |        OR growths['variability_count_page_redirect'] < -${thresholdsConfig.variability_count_page_redirect_max}
         |        OR growths['variability_count_page_redirect'] > ${thresholdsConfig.variability_count_page_redirect_max}
         |
         |        OR growths['growth_count_revisions'] < ${thresholdsConfig.growth_count_revisions_min}
         |        OR growths['growth_count_revisions'] > ${thresholdsConfig.growth_count_revisions_max}
         |
         |        -- Variability not growth --> between -max and max
         |        OR growths['variability_sum_text_bytes_diff'] < -${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |        OR growths['variability_sum_text_bytes_diff'] > ${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |
         |        OR growths['growth_sum_text_bytes_diff_abs'] < ${thresholdsConfig.growth_sum_text_bytes_diff_abs_min}
         |        OR growths['growth_sum_text_bytes_diff_abs'] > ${thresholdsConfig.growth_sum_text_bytes_diff_abs_max}
         |    ))
         |
         |    -- Revision values
         |    OR (event_entity = 'revision' AND (
         |        growths['growth_count_revision_deleted'] < ${thresholdsConfig.growth_count_revision_deleted_min}
         |        OR growths['growth_count_revision_deleted'] > ${thresholdsConfig.growth_count_revision_deleted_max}
         |
         |    ))
      """.stripMargin).cache()
  }

  /**
   *
   * @param userMetricsGrowth
   * @return User growth error ratio
   */
  def getReducedGrowthErrorsRatio(reducedMetricsGrowth: DataFrame): Double = {
    val compliancePredicate: String =
      s"""growths['growth_count_reduced_event'] < ${thresholdsConfig.growth_count_reduced_event_min}
         |OR growths['growth_count_reduced_event'] > ${thresholdsConfig.growth_count_reduced_event_max}
         |OR (event_entity = 'user' AND(
         |    growths['growth_distinct_user_text'] < ${thresholdsConfig.growth_distinct_user_text_min}
         |    OR growths['growth_distinct_user_text'] > ${thresholdsConfig.growth_distinct_user_text_max}
         |    OR growths['growth_count_user_group_bot'] < ${thresholdsConfig.growth_count_user_group_bot_min}
         |    OR growths['growth_count_user_group_bot'] > ${thresholdsConfig.growth_count_user_group_bot_max}
         |    OR growths['growth_count_user_name_bot'] < ${thresholdsConfig.growth_count_user_name_bot_min}
         |    OR growths['growth_count_user_name_bot'] > ${thresholdsConfig.growth_count_user_name_bot_max}
         |    OR growths['growth_count_user_anonymous'] < ${thresholdsConfig.growth_count_user_anonymous_min}
         |    OR growths['growth_count_user_anonymous'] > ${thresholdsConfig.growth_count_user_anonymous_max}
         |    OR growths['growth_count_user_user'] < ${thresholdsConfig.growth_count_user_user_min}
         |    OR growths['growth_count_user_user'] > ${thresholdsConfig.growth_count_user_user_max}
         |    OR growths['growth_count_user_self_created'] < ${thresholdsConfig.growth_count_user_self_created_min}
         |    OR growths['growth_count_user_self_created'] > ${thresholdsConfig.growth_count_user_self_created_max}
         |    OR growths['growth_count_revisions'] < ${thresholdsConfig.growth_count_revisions_min}
         |    OR growths['growth_count_revisions'] > ${thresholdsConfig.growth_count_revisions_max}
         |    OR growths['variability_sum_text_bytes_diff'] < -${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |    OR growths['variability_sum_text_bytes_diff'] > ${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |    OR growths['growth_sum_text_bytes_diff_abs'] < ${thresholdsConfig.growth_sum_text_bytes_diff_abs_min}
         |    OR growths['growth_sum_text_bytes_diff_abs'] > ${thresholdsConfig.growth_sum_text_bytes_diff_abs_max}))
         |OR (event_entity = 'page' AND (
         |    growths['growth_distinct_page_title'] < ${thresholdsConfig.growth_distinct_page_title_min}
         |    OR growths['growth_distinct_page_title'] > ${thresholdsConfig.growth_distinct_page_title_max}
         |    OR growths['growth_distinct_page_namespace'] < ${thresholdsConfig.growth_distinct_page_namespace_min}
         |    OR growths['growth_distinct_page_namespace'] > ${thresholdsConfig.growth_distinct_page_namespace_max}
         |    OR growths['growth_count_page_content'] < ${thresholdsConfig.growth_count_page_content_min}
         |    OR growths['growth_count_page_content'] > ${thresholdsConfig.growth_count_page_content_max}
         |    OR growths['growth_count_page_non_content'] < ${thresholdsConfig.growth_count_page_non_content_min}
         |    OR growths['growth_count_page_non_content'] > ${thresholdsConfig.growth_count_page_non_content_max}
         |    OR growths['variability_count_page_redirect'] < -${thresholdsConfig.variability_count_page_redirect_max}
         |    OR growths['variability_count_page_redirect'] > ${thresholdsConfig.variability_count_page_redirect_max}
         |    OR growths['growth_count_revisions'] < ${thresholdsConfig.growth_count_revisions_min}
         |    OR growths['growth_count_revisions'] > ${thresholdsConfig.growth_count_revisions_max}
         |    OR growths['variability_sum_text_bytes_diff'] < -${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |    OR growths['variability_sum_text_bytes_diff'] > ${thresholdsConfig.variability_sum_text_bytes_diff_max}
         |    OR growths['growth_sum_text_bytes_diff_abs'] < ${thresholdsConfig.growth_sum_text_bytes_diff_abs_min}
         |    OR growths['growth_sum_text_bytes_diff_abs'] > ${thresholdsConfig.growth_sum_text_bytes_diff_abs_max}))
         |OR (event_entity = 'revision' AND (
         |    growths['growth_count_revision_deleted'] < ${thresholdsConfig.growth_count_revision_deleted_min}
         |    OR growths['growth_count_revision_deleted'] > ${thresholdsConfig.growth_count_revision_deleted_max}))""".stripMargin.replaceAll("\n", " ")

    columnComplianceAnalysis(reducedMetricsGrowth, compliancePredicate, "Check Reduced ErrorRatio Metric")
  }

  def checkReducedHistory(): Unit = {
    val previousReducedHistory = spark.read.parquet(previousReducedHistoryPath)
    val newReducedHistory = spark.read.parquet(newReducedHistoryPath)

    val previousReducedMetrics = getReducedMetrics(previousReducedHistory, previousSnapshot)
    val newReducedMetrics = getReducedMetrics(newReducedHistory, newSnapshot)

    val projectsToCheck = getProjectsToCheck(newReducedMetrics)

    val reducedMetricsGrowth = getReducedMetricsGrowth(
      previousReducedMetrics.where(col("project").isin(projectsToCheck:_*)),
      newReducedMetrics.where(col("project").isin(projectsToCheck:_*))
    )

    val errorRowsRatio = getReducedGrowthErrorsRatio(reducedMetricsGrowth)

    log.info(s"ReducedMetricsGrowthErrors ratio: $errorRowsRatio")

    if (errorRowsRatio > thresholdsConfig.reducedWrongRowsRatioThreshold) {
      log.warn(s"ReducedMetricsGrowthErrors ratio $errorRowsRatio is higher " +
        s"than expected threshold ${thresholdsConfig.reducedWrongRowsRatioThreshold} -- Writing errors")
      val reducedMetricsGrowthErrors = getReducedMetricsGrowthErrors(reducedMetricsGrowth)
      reducedMetricsGrowthErrors.repartition(1).write.mode(SaveMode.Overwrite).json(outputPath)
    }

  }

}
