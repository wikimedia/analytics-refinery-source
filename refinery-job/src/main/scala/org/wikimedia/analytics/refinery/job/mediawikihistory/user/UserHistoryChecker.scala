package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wikimedia.analytics.refinery.job.mediawikihistory.{DeequColumnAnalysis, MediawikiHistoryCheckerConfig}
import org.wikimedia.analytics.refinery.tools.LogHelper

/**
  * Class checking a mediawiki-user-history snapshot versus a previously generated one (expected correct).
  * Used by [[org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryChecker]]
  */
class UserHistoryChecker(
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
  private val previousUserHistoryPath = s"$mediawikiHistoryBasePath/user_history/snapshot=$previousSnapshot"
  private val newUserHistoryPath = s"$mediawikiHistoryBasePath/user_history/snapshot=$newSnapshot"

  /**
   * User metrics for a snapshot (works for both previous and new)
   */
  def getUserMetrics(userSnapshot: DataFrame, snapshot: String): DataFrame = {
    val tmpTable = "tmp_user"
    userSnapshot.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
        |SELECT
        |    wiki_db AS wiki_db,
        |    caused_by_event_type AS caused_by_event_type,
        |    COUNT(1) AS count_user_event,
        |    COUNT(DISTINCT user_id) AS distinct_user_id,
        |    COUNT(DISTINCT user_text) AS distinct_user_text,
        |    SUM(IF(ARRAY_CONTAINS(user_groups_historical, "bot"), 1, 0)) AS count_user_group_bot,
        |    SUM(IF(is_anonymous, 1, 0)) AS count_user_anonymous,
        |    SUM(IF(is_temporary, 1, 0)) AS count_user_temporary,
        |    SUM(IF(is_permanent, 1, 0)) AS count_user_permanent,
        |    SUM(IF(created_by_self, 1, 0)) AS count_user_self_created
        |FROM $tmpTable
        |-- Null start_timestamp means beginning of time, therefore before snapshot :)
        |WHERE (start_timestamp IS NULL OR SUBSTR(start_timestamp, 0, 7) <= '$snapshot')
        |GROUP BY
        |    wiki_db,
        |    caused_by_event_type
      """.stripMargin)
  }

  def getWikisToCheck(userMetrics: DataFrame): Seq[String] = {
    val tmpTable = "tmp_user_metrics"
    userMetrics.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
         |SELECT
         |    wiki_db,
         |    SUM(count_user_event) AS count_user_event
         |FROM $tmpTable
         |GROUP BY
         |    wiki_db
         |ORDER BY count_user_event DESC
         |LIMIT $wikisToCheck
      """.stripMargin).collect()
      .map(_.getString(0))
  }

  /**
   * User Metrics Growth from the previous snapshot to the new one
   */
  def getUserMetricsGrowth(previousUserMetrics: DataFrame, newUserMetrics: DataFrame): DataFrame = {
    val tmpPrevTable = "tmp_prev_user_metrics"
    val tmpNewTable = "tmp_new_user_metrics"
    previousUserMetrics.createOrReplaceTempView(tmpPrevTable)
    newUserMetrics.createOrReplaceTempView(tmpNewTable)
    spark.sql(
      s"""
        |SELECT
        |    COALESCE(p.wiki_db, n.wiki_db) AS wiki_db,
        |    'userHistory' AS event_entity,
        |    COALESCE(p.caused_by_event_type, n.caused_by_event_type) AS event_type,
        |    MAP(
        |        'growth_count_user_event',
        |            (COALESCE(n.count_user_event, 0) - COALESCE(p.count_user_event, 0)) / COALESCE(NULLIF(p.count_user_event, 0), 1),
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
        |            (COALESCE(n.count_user_self_created, 0) - COALESCE(p.count_user_self_created, 0)) / COALESCE(NULLIF(p.count_user_self_created, 0), 1)
        |    ) AS growths
        |FROM $tmpPrevTable p
        |    FULL OUTER JOIN $tmpNewTable n
        |        ON ((p.wiki_db = n.wiki_db)
        |            AND (p.caused_by_event_type = n.caused_by_event_type))
      """.stripMargin).cache()
  }

  /**
   * User Metrics Growth Errors - When outside threshold boundaries
   */
  def getUserMetricsGrowthErrors(userMetricsGrowth: DataFrame): DataFrame = {
    val tmpTable = "tmp_user_metrics_growth"
    userMetricsGrowth.createOrReplaceTempView(tmpTable)
    spark.sql(
      s"""
        |SELECT
        |    wiki_db,
        |    event_entity,
        |    event_type,
        |    growths
        |FROM $tmpTable
        |WHERE growths['growth_count_user_event'] < ${thresholdsConfig.growth_count_user_event_min}
        |    OR growths['growth_count_user_event'] > ${thresholdsConfig.growth_count_user_event_max}
        |
        |    OR growths['growth_distinct_user_id'] < ${thresholdsConfig.growth_distinct_user_id_min}
        |    OR growths['growth_distinct_user_id'] > ${thresholdsConfig.growth_distinct_user_id_max}
        |
        |    OR growths['growth_distinct_user_text'] < ${thresholdsConfig.growth_distinct_user_text_min}
        |    OR growths['growth_distinct_user_text'] > ${thresholdsConfig.growth_distinct_user_text_max}
        |
        |    OR growths['growth_count_user_group_bot'] < ${thresholdsConfig.growth_count_user_group_bot_min}
        |    OR growths['growth_count_user_group_bot'] > ${thresholdsConfig.growth_count_user_group_bot_max}
        |
        |    OR growths['growth_count_user_anonymous'] < ${thresholdsConfig.growth_count_user_anonymous_min}
        |    OR growths['growth_count_user_anonymous'] > ${thresholdsConfig.growth_count_user_anonymous_max}
        |
        |    OR growths['growth_count_user_temporary'] < ${thresholdsConfig.growth_count_user_temporary_min}
        |    OR growths['growth_count_user_temporary'] > ${thresholdsConfig.growth_count_user_temporary_max}
        |
        |    OR growths['growth_count_user_permanent'] < ${thresholdsConfig.growth_count_user_permanent_min}
        |    OR growths['growth_count_user_permanent'] > ${thresholdsConfig.growth_count_user_permanent_max}
        |
        |    OR growths['growth_count_user_self_created'] < ${thresholdsConfig.growth_count_user_self_created_min}
        |    OR growths['growth_count_user_self_created'] > ${thresholdsConfig.growth_count_user_self_created_max}
      """.stripMargin).cache()
  }

  /**
   *
   * @param userMetricsGrowth
   * @return User growth error ratio
   */
  def getUserGrowthErrorsRatio(userMetricsGrowth: DataFrame):Double = {
    val compliancePredicate:String =
      s"""growths['growth_count_user_event'] < ${thresholdsConfig.growth_count_user_event_min}
         |OR growths['growth_count_user_event'] > ${thresholdsConfig.growth_count_user_event_max}
         |OR growths['growth_distinct_user_id'] < ${thresholdsConfig.growth_distinct_user_id_min}
         |OR growths['growth_distinct_user_id'] > ${thresholdsConfig.growth_distinct_user_id_max}
         |OR growths['growth_distinct_user_text'] < ${thresholdsConfig.growth_distinct_user_text_min}
         |OR growths['growth_distinct_user_text'] > ${thresholdsConfig.growth_distinct_user_text_max}
         |OR growths['growth_count_user_group_bot'] < ${thresholdsConfig.growth_count_user_group_bot_min}
         |OR growths['growth_count_user_group_bot'] > ${thresholdsConfig.growth_count_user_group_bot_max}
         |OR growths['growth_count_user_anonymous'] < ${thresholdsConfig.growth_count_user_anonymous_min}
         |OR growths['growth_count_user_anonymous'] > ${thresholdsConfig.growth_count_user_anonymous_max}
         |OR growths['growth_count_user_temporary'] < ${thresholdsConfig.growth_count_user_temporary_min}
         |OR growths['growth_count_user_temporary'] > ${thresholdsConfig.growth_count_user_temporary_max}
         |OR growths['growth_count_user_permanent'] < ${thresholdsConfig.growth_count_user_permanent_min}
         |OR growths['growth_count_user_permanent'] > ${thresholdsConfig.growth_count_user_permanent_max}
         |OR growths['growth_count_user_self_created'] < ${thresholdsConfig.growth_count_user_self_created_min}
         |OR growths['growth_count_user_self_created'] > ${thresholdsConfig.growth_count_user_self_created_max}""".stripMargin.replaceAll("\n", " ")

    columnComplianceAnalysis(userMetricsGrowth, compliancePredicate, "Check User ErrorRatio metric")
  }

  def checkUserHistory(): Unit = {

    val previousUserHistory = spark.read.parquet(previousUserHistoryPath)
    val newUserHistory = spark.read.parquet(newUserHistoryPath)

    val previousUserMetrics = getUserMetrics(previousUserHistory, previousSnapshot)
    val newUserMetrics = getUserMetrics(newUserHistory, newSnapshot)

    val wikisToCheck = getWikisToCheck(newUserMetrics)

    val userMetricsGrowth = getUserMetricsGrowth(
      previousUserMetrics.where(col("wiki_db").isin(wikisToCheck:_*)),
      newUserMetrics.where(col("wiki_db").isin(wikisToCheck:_*))
    )

    val errorRowsRatio = getUserGrowthErrorsRatio(userMetricsGrowth)
    log.info(s"UserMetricsGrowthErrors ratio: $errorRowsRatio")

    if (errorRowsRatio > thresholdsConfig.userWrongRowsRatioThreshold) {
      log.warn(s"UserMetricsGrowthErrors ratio $errorRowsRatio is higher " +
        s"than expected threshold ${thresholdsConfig.userWrongRowsRatioThreshold} -- Writing errors")
      val userMetricsGrowthErrors = getUserMetricsGrowthErrors(userMetricsGrowth)
      userMetricsGrowthErrors.repartition(1).write.mode(SaveMode.Append).json(outputPath)
    }

  }

}