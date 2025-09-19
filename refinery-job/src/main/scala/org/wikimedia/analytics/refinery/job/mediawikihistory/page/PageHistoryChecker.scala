package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.wikimedia.analytics.refinery.job.mediawikihistory.{DeequColumnAnalysis, MediawikiHistoryCheckerConfig}
import org.wikimedia.analytics.refinery.tools.LogHelper

/**
 * Class checking a mediawiki-page-history snapshot versus a previously generated one (expected correct).
 * Used by [[org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryChecker]]
 */
class PageHistoryChecker(
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
  val outputPath = s"$mediawikiHistoryBasePath/history_check_errors/snapshot=$newSnapshot"
  private val previousPageHistoryPath = s"$mediawikiHistoryBasePath/page_history/snapshot=$previousSnapshot"
  private val newPageHistoryPath = s"$mediawikiHistoryBasePath/page_history/snapshot=$newSnapshot"

  /**
   * Page metrics for a snapshot (works for both previous and new)
   */
  def getPageMetrics(pageSnapshot: DataFrame, snapshot: String): DataFrame = {
    val tmpTable = "tmp_page"
    pageSnapshot.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db AS wiki_db,
         |    caused_by_event_type AS caused_by_event_type,
         |    COUNT(1) AS count_page_event,
         |    COUNT(DISTINCT page_id) AS distinct_page_id,
         |    COUNT(DISTINCT page_artificial_id) AS distinct_page_artificial_id,
         |    COUNT(DISTINCT COALESCE(page_title_historical, page_title)) AS distinct_page_title,
         |    COUNT(DISTINCT COALESCE(page_namespace_historical, page_namespace)) AS distinct_page_namespace,
         |    SUM(IF(page_is_redirect, 1, 0)) AS count_page_redirect
         |FROM $tmpTable
         |-- Null start_timestamp means beginning of time, therefore before snapshot :)
         |WHERE (start_timestamp IS NULL OR SUBSTR(start_timestamp, 0, 7) <= '$snapshot')
         |GROUP BY
         |    wiki_db,
         |    caused_by_event_type
      """.stripMargin).cache()
  }

  def getWikisToCheck(pageMetrics: DataFrame): Seq[String] = {
    val tmpTable = "tmp_page_metrics"
    pageMetrics.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db,
         |    SUM(count_page_event) AS count_page_event
         |FROM $tmpTable
         |GROUP BY
         |    wiki_db
         |ORDER BY count_page_event DESC
         |LIMIT $wikisToCheck
      """.stripMargin).collect()
      .map(_.getString(0))
  }

  /**
   * Page Metrics Growth from the previous snapshot to the new one
   */
  def getPageMetricsGrowth(previousPageMetrics: DataFrame, newPageMetrics: DataFrame): DataFrame = {
    val tmpPrevTable = "tmp_prev_page_metrics"
    val tmpNewTable = "tmp_new_page_metrics"
    previousPageMetrics.createOrReplaceTempView(tmpPrevTable)
    newPageMetrics.createOrReplaceTempView(tmpNewTable)
    spark.sql(
      s"""
         |SELECT
         |    COALESCE(p.wiki_db, n.wiki_db) AS wiki_db,
         |    'pageHistory' AS event_entity,
         |    COALESCE(p.caused_by_event_type, n.caused_by_event_type) AS event_type,
         |    MAP(
         |        'growth_count_page_event',
         |            (COALESCE(n.count_page_event, 0) - COALESCE(p.count_page_event, 0)) / COALESCE(NULLIF(p.count_page_event, 0), 1),
         |        'growth_distinct_all_page_id',
         |            ((COALESCE(n.distinct_page_id, 0) + COALESCE(n.distinct_page_artificial_id, 0)) -
         |              (COALESCE(p.distinct_page_id, 0) + COALESCE(p.distinct_page_artificial_id, 0))) /
         |              (COALESCE(NULLIF(p.distinct_page_id, 0), 1) + COALESCE(NULLIF(p.distinct_page_artificial_id, 0), 1)),
         |        'growth_distinct_page_id',
         |            (COALESCE(n.distinct_page_id, 0) - COALESCE(p.distinct_page_id, 0)) / COALESCE(NULLIF(p.distinct_page_id, 0), 1),
         |        'growth_distinct_page_artificial_id',
         |            (COALESCE(n.distinct_page_artificial_id, 0) - COALESCE(p.distinct_page_artificial_id, 0)) / COALESCE(NULLIF(p.distinct_page_artificial_id, 0), 1),
         |        'growth_distinct_page_title',
         |            (COALESCE(n.distinct_page_title, 0) - COALESCE(p.distinct_page_title, 0)) / COALESCE(NULLIF(p.distinct_page_title, 0), 1),
         |        'growth_distinct_page_namespace',
         |            (COALESCE(n.distinct_page_namespace, 0) - COALESCE(p.distinct_page_namespace, 0)) / COALESCE(NULLIF(p.distinct_page_namespace, 0), 1),
         |        -- Special case for count_page_redirect: Since this value is set from the current state of pages,
         |        -- there is no historical aspect to it, therefore we measure it;s variability month to month,
         |        -- not its growth.
         |        'variability_count_page_redirect',
         |            (COALESCE(n.count_page_redirect, 0) - COALESCE(p.count_page_redirect, 0)) / COALESCE(NULLIF(p.count_page_redirect, 0), 1)
         |    ) AS growths
         |FROM $tmpPrevTable p
         |    FULL OUTER JOIN $tmpNewTable n
         |        ON ((p.wiki_db = n.wiki_db)
         |            AND (p.caused_by_event_type = n.caused_by_event_type))
      """.stripMargin).cache()
  }

  /**
   * Page Metrics Growth Errors - When outside threshold boundaries
   */
  def getPageMetricsGrowthErrors(pageMetricsGrowth: DataFrame): DataFrame = {
    val tmpTable = "tmp_page_metrics_growth"
    pageMetricsGrowth.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db,
         |    event_entity,
         |    event_type,
         |    growths
         |FROM $tmpTable
         |WHERE growths['growth_count_page_event'] < ${thresholdsConfig.growth_count_page_event_min}
         |        OR growths['growth_count_page_event'] > ${thresholdsConfig.growth_count_page_event_max}
         |
         |        OR growths['growth_distinct_all_page_id'] < ${thresholdsConfig.growth_distinct_all_page_id_min}
         |        OR growths['growth_distinct_all_page_id'] > ${thresholdsConfig.growth_distinct_all_page_id_max}
         |
         |        OR growths['growth_distinct_page_title'] < ${thresholdsConfig.growth_distinct_page_title_min}
         |        OR growths['growth_distinct_page_title'] > ${thresholdsConfig.growth_distinct_page_title_max}
         |
         |        OR growths['growth_distinct_page_namespace'] < ${thresholdsConfig.growth_distinct_page_namespace_min}
         |        OR growths['growth_distinct_page_namespace'] > ${thresholdsConfig.growth_distinct_page_namespace_max}
         |
         |        -- Since we measure variability, we set the lower accepted threshold limit to
         |        -- -maxEventsGrowthThreshold.
         |        OR growths['variability_count_page_redirect'] < -${thresholdsConfig.variability_count_page_redirect_max}
         |        OR growths['variability_count_page_redirect'] > ${thresholdsConfig.variability_count_page_redirect_max}
      """.stripMargin).cache()
  }

  /**
   *
   * @param pageMetricsGrowth
   * @return Page History growth error ratio
   */
  def getPageGrowthErrorsRatio(pageMetricsGrowth: DataFrame): Double = {
    val compliancePredicate: String =
      s"""
        |growths['growth_count_page_event'] < ${thresholdsConfig.growth_count_page_event_min}
        |OR growths['growth_count_page_event'] > ${thresholdsConfig.growth_count_page_event_max}
        |OR growths['growth_distinct_all_page_id'] < ${thresholdsConfig.growth_distinct_all_page_id_min}
        |OR growths['growth_distinct_all_page_id'] > ${thresholdsConfig.growth_distinct_all_page_id_max}
        |OR growths['growth_distinct_page_title'] < ${thresholdsConfig.growth_distinct_page_title_min}
        |OR growths['growth_distinct_page_title'] > ${thresholdsConfig.growth_distinct_page_title_max}
        |OR growths['growth_distinct_page_namespace'] < ${thresholdsConfig.growth_distinct_page_namespace_min}
        |OR growths['growth_distinct_page_namespace'] > ${thresholdsConfig.growth_distinct_page_namespace_max}
        |OR growths['variability_count_page_redirect'] < -${thresholdsConfig.variability_count_page_redirect_max}
        |OR growths['variability_count_page_redirect'] > ${thresholdsConfig.variability_count_page_redirect_max}
        |""".stripMargin.replaceAll("\n", " ")

    columnComplianceAnalysis(pageMetricsGrowth, compliancePredicate, "Check Page ErrorRatio Metric")
  }

  def checkPageHistory(): Unit = {
    val previousPageHistory = spark.read.parquet(previousPageHistoryPath)
    val newPageHistory = spark.read.parquet(newPageHistoryPath)

    val previousPageMetrics = getPageMetrics(previousPageHistory, previousSnapshot)
    val newPageMetrics = getPageMetrics(newPageHistory, newSnapshot)

    val wikisToCheck = getWikisToCheck(newPageMetrics)

    val pageMetricsGrowth = getPageMetricsGrowth(
      previousPageMetrics.where(col("wiki_db").isin(wikisToCheck:_*)),
      newPageMetrics.where(col("wiki_db").isin(wikisToCheck:_*))
    )

    //val pageFalsePositives = getPageFalsePositives(pageMetricsGrowth)

    val errorRowsRatio = getPageGrowthErrorsRatio(pageMetricsGrowth)
    log.info(s"PageMetricsGrowthErrors ratio: $errorRowsRatio")

    if (errorRowsRatio > thresholdsConfig.pageWrongRowsRatioThreshold) {
      log.warn(s"PageMetricsGrowthErrors ratio $errorRowsRatio is higher " +
        s"than expected threshold ${thresholdsConfig.pageWrongRowsRatioThreshold} -- Writing errors")
      val pageMetricsGrowthErrors = getPageMetricsGrowthErrors(pageMetricsGrowth)
      pageMetricsGrowthErrors.repartition(1).write.mode(SaveMode.Append).json(outputPath)
    }


  }

}
