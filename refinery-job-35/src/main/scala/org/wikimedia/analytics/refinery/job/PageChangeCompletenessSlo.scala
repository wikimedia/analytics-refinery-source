package org.wikimedia.analytics.refinery.job

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import org.wikimedia.analytics.refinery.spark.utils.SparkLogHelper
import org.wikimedia.analytics.refinery.tools.config._

object PageChangeCompletenessSlo extends SparkLogHelper with ConfigHelper {

  // Field names are the CLI argument names, as ConfigHelper maps them one to one.
  // Fields without a default value are required.
  case class Params(
                     source_table_raw_revision: String,  // partitioned by day
                     source_table_raw_logging: String,  // partitioned by day
                     source_table_event_page_change: String,  // event.mediawiki_page_change_v1
                     target_table_metric: String,  // wmf_data_ops.data_quality_metrics
                     target_table_alert: String   = "",  // wmf_data_ops.data_quality_alerts
                     alert_hdfs_path: String   = "",  // path where to load file in case of an alert
                     airflow_run_id: String,  // run id of the Airflow DAG
                     execution_date: String,  // day to measure and partition of the metric rows, '2026-09-01'
                     mode: String,  // 'Completeness'
                     alert_threshold: String,  // 0.99
                     all_wiki_id: String,  // 'all-wikis'
                     prometheus_metric: String  // prometheus root name of the metrics
                   )

  // $COVERAGE-OFF$
  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val spark = SparkSession.builder().getOrCreate()
    adjustLoggingLevelToSparkMaster(spark)
    run(spark, params)
    spark.stop()
  }
  // $COVERAGE-ON$

  def parseArgs(args: Array[String]): Params = configureArgs[Params](args)


  def run(spark: SparkSession, p: Params): Unit = {
    log.info(s"Page Change Completeness SLO params: $p")
    val sourceTableEventPageChangeName = p.source_table_event_page_change.replace("`", "").split("\\.").last

    runCleanupSQL(p,spark,sourceTableEventPageChangeName)

    val insertSQL = buildInsertSQL(p,sourceTableEventPageChangeName)
    log.info(s"Running PageChangeCompletenessSlo INSERT INTO ${p.target_table_metric}:\n$insertSQL")
    spark.sql(insertSQL).collect()

    checkSnapshotNotEmpty(p, spark, sourceTableEventPageChangeName)

    val alertSQL = buildAlertSQL(p)
    val alertDf = spark.sql(alertSQL)
    if(p.target_table_alert!="") {
      if(!alertDf.isEmpty) {
        alertDf.writeTo(p.target_table_alert).append()
        log.info(s"Running PageChangeCompletenessSlo INSERT INTO ${p.target_table_alert}")
        if(p.alert_hdfs_path!=""){
          log.info(s"Running PageChangeCompletenessSlo EXPORT ALERT TO ${p.alert_hdfs_path}")
          exportAlert(p, alertDf, spark)
        }
      }else{
        log.info(s"Running PageChangeCompletenessSlo no alert to write.")
      }

    }
    log.info(s"Running PageChangeCompletenessSlo export metrics to Prometheus.")
    exportMetricToPrometheus(p, spark, sourceTableEventPageChangeName)


  }

  /**
   * Fails the job when the snapshot of the execution date has no rows.
   * An empty snapshot gives a NULL ratio, which never fires an alert. This makes a failed
   * Sqoop import visible as a failed task instead of a healthy day.
   */
  def checkSnapshotNotEmpty(p: Params, spark: SparkSession, sourceTableEventPageChangeName: String): Unit = {
    val query =
      s"""SELECT tags['total_count'] AS total_count
         FROM ${p.target_table_metric}
         WHERE partition_ts = to_timestamp('${p.execution_date}')
           AND name = '${p.mode}'
           AND source_table = '${quotedTableName(p.source_table_event_page_change)}'
           AND pipeline_run_id = '${p.airflow_run_id}'
           AND tags['wiki_id'] = '${p.all_wiki_id}'
           AND tags['project'] = '${sourceTableEventPageChangeName}_slo'"""
    val totals = spark.sql(query).collect().map(r => Option(r.getString(0)).map(_.toDouble).getOrElse(0.0))
    if (totals.isEmpty || totals.forall(_ == 0.0)) {
      val message = s"No revision or logging rows in the snapshot of ${p.execution_date}: " +
        s"${p.source_table_raw_revision} and ${p.source_table_raw_logging} are empty for that day. " +
        "The completeness ratio is undefined, so the job fails."
      log.error(message)
      throw new IllegalStateException(message)
    }
  }

  /**
   * Deletes stale rows for the execution date before the fresh INSERT.
   * Removes metric rows and alert rows that match the partition, source table, mode, and project.
   */
  def runCleanupSQL(p: Params, spark:SparkSession, sourceTableEventPageChangeName: String): Unit = {

    val cleanMetric =
      s"""DELETE FROM ${p.target_table_metric}
WHERE  partition_ts = to_timestamp('${p.execution_date}')
       AND source_table='${quotedTableName(p.source_table_event_page_change)}'
       AND name= '${p.mode}'
       AND tags.project = '${sourceTableEventPageChangeName}_slo'"""
    log.info(s"Running PageChangeCompletenessSlo Metric cleanup DELETE:\n$cleanMetric")
    spark.sql(cleanMetric).collect()

    // The alert table is optional. Without it, there are no alert rows to clean.
    if (p.target_table_alert != "") {
      val cleanAlert =
        s"""DELETE FROM ${p.target_table_alert}
WHERE  partition_ts = to_timestamp('${p.execution_date}')
       AND source_table='${quotedTableName(p.source_table_event_page_change)}'
       AND constraint= '${p.mode}'
       AND tags.project = '${sourceTableEventPageChangeName}_slo'"""
      log.info(s"Running PageChangeCompletenessSlo Alert cleanup DELETE:\n$cleanAlert")

      spark.sql(cleanAlert).collect()
    }
  }

  /**
   * Returns the table name as `schema`.`table`, the format stored in the source_table column,
   * for example `event`.`mediawiki_page_change_v1`. It accepts the name with or without backticks.
   */
  def quotedTableName(table: String): String =
    table.replace("`", "").split("\\.").map(part => s"`$part`").mkString(".")

  /** Returns the Iceberg partition_id expression for the given execution date. */
  def generatePartitionId(executionDate: String): String = {
    val d = java.time.LocalDate.parse(executionDate)
    s"concat('wiki_id=',wiki_id,'/year=${d.getYear}/month=${d.getMonthValue}/day=${d.getDayOfMonth}')"
  }

  /**
   * Returns a SELECT that computes the completeness ratio of one day, per wiki.
   * The day is the execution date. It reads the daily revision and logging tables and the
   * page_change events of that day, and finds the entries that are missing from the events:
   *   - revisions are matched on (wiki_id, revision_id, change_type).
   *   - page deletes and restores are matched on (wiki_id, page_id, change_type).
   * The result has one row per wiki and one row for all wikis (wiki_id = allWikiId).
   * The value is the ratio of all change types together. The tags carry the missing and total
   * counts of every change type (revision, delete, restore).
   * Deduplicates on (wiki_id, revision_id) in both revision sources.
   * change_type is an artificial field created to split over revision, delete, and restore events.
   */
  def buildSelectSQL(p: Params, sourceTableEventPageChangeName: String): String = {
    val day    = java.time.LocalDate.parse(p.execution_date)
    val dayStr = day.toString                                   // 2026-08-21
      s"""
    with events as (
        select
            distinct revision.rev_id as revision_id,
            page.page_id as page_id,
            wiki_id as wiki_id,
            case when changelog_kind = 'delete' and page_change_kind='delete' then 'delete'
                 when changelog_kind = 'insert' and page_change_kind='undelete' then 'restore'
                 else 'revision' end as change_type
        from
            ${p.source_table_event_page_change}
        where
            year=${day.getYear}
            and month=${day.getMonthValue}
            and day=${day.getDayOfMonth}
        ),
    snapshot_data as (
        select
            distinct rev_id as revision_id,
            rev_page as page_id,
            wiki_db as wiki_id,
            'revision' as change_type
        from
            ${p.source_table_raw_revision}
        where
            day = '${dayStr}'
            and TO_DATE(rev_timestamp, 'yyyyMMddHHmmss') = '${dayStr}'
        union all
        select
            null as revision_id,
            log_page as page_id,
            wiki_db as wiki_id,
            log_action as change_type
        from
            ${p.source_table_raw_logging}
        where
            day = '${dayStr}'
            and TO_DATE(log_timestamp, 'yyyyMMddHHmmss') = '${dayStr}'
            and log_type = 'delete'
            and log_action in ('delete', 'restore')
    ),
    snapshot_counts as (
        select
            count(*) as snapshot_count,
            wiki_id,
            change_type
        from
            snapshot_data
        group by
            wiki_id,
            change_type
    ),
    missing_delete_restore as (
        select
            sd.wiki_id,
            sd.change_type
        from
            snapshot_data sd
        left anti join
            events e
        on sd.wiki_id = e.wiki_id
           and sd.page_id = e.page_id
           and sd.change_type = e.change_type
        where sd.change_type in ('delete', 'restore')
    ),
    missing_revisions as (
        select
            sd.wiki_id,
            sd.change_type
        from
            snapshot_data sd
        left anti join
            events e
        on sd.wiki_id = e.wiki_id
           and sd.revision_id = e.revision_id
           and sd.change_type = e.change_type
        where sd.change_type = 'revision'
    ),
    missing_counts as (
        select
            wiki_id,
            change_type,
            count(*) as count_event_missing
        from (
            select wiki_id, change_type from missing_delete_restore
            union all
            select wiki_id, change_type from missing_revisions
        )
        group by
            wiki_id,
            change_type
    ),
    counts_by_wiki_change_type as (
        select
            sc.wiki_id,
            sc.change_type,
            sc.snapshot_count,
            coalesce(m.count_event_missing, 0) as count_event_missing
        from snapshot_counts sc
        left join missing_counts m
        using (wiki_id, change_type)
    ),
    total_sum_per_wiki as (
        -- one row per wiki, with the counts of every change type
        select
            wiki_id,
            sum(count_event_missing) as count_total_event_missing,
            sum(case when change_type = 'revision' then count_event_missing else 0 end) as count_revision_missing,
            sum(case when change_type = 'delete'   then count_event_missing else 0 end) as count_delete_missing,
            sum(case when change_type = 'restore'  then count_event_missing else 0 end) as count_restore_missing,
            sum(snapshot_count) as total_count,
            sum(case when change_type = 'revision' then snapshot_count else 0 end) as revision_count,
            sum(case when change_type = 'delete'   then snapshot_count else 0 end) as delete_count,
            sum(case when change_type = 'restore'  then snapshot_count else 0 end) as restore_count
        from counts_by_wiki_change_type
        group by wiki_id
    ),
    total as (
        -- per wiki
        select
            wiki_id,
            '${dayStr}' as revision_date,
            count_total_event_missing,
            count_revision_missing,
            count_delete_missing,
            count_restore_missing,
            total_count,
            revision_count,
            delete_count,
            restore_count,
            1 - (count_total_event_missing / total_count) as ratio
        from total_sum_per_wiki

        union all
        -- all wikis
        select
            '${p.all_wiki_id}' as wiki_id,
            '${dayStr}' as revision_date,
            sum(count_total_event_missing),
            sum(count_revision_missing),
            sum(count_delete_missing),
            sum(count_restore_missing),
            sum(total_count),
            sum(revision_count),
            sum(delete_count),
            sum(restore_count),
            1 - (sum(count_total_event_missing) / sum(total_count))
        from total_sum_per_wiki
    )
    SELECT
            ${java.time.Instant.now().toEpochMilli} as dataset_date,
            map(
                'computation_class', 'all-of-wiki-time',
                'project', '${sourceTableEventPageChangeName}_slo',
                'wiki_id', wiki_id,
                'max_timestamp', revision_date,
                'missing_events', count_total_event_missing,
                'missing_revisions', count_revision_missing,
                'missing_deletes', count_delete_missing,
                'missing_restores', count_restore_missing,
                'total_count', total_count,
                'revision_count', revision_count,
                'delete_count', delete_count,
                'restore_count', restore_count
                ) AS tags,
            'Dataset' AS entity,
            '*' AS instance,
            ratio as value,
            '${p.mode}' as name,
            '${quotedTableName(p.source_table_event_page_change)}' AS source_table,
            '${p.airflow_run_id}' as pipeline_run_id,
            ${generatePartitionId(p.execution_date)} as partition_id,
            to_timestamp('${p.execution_date}') as partition_ts
        FROM
            total
    """
  }

  /**
   * Returns an INSERT INTO the metric table with the completeness ratio computed by buildSelectSQL.
   * Uses delete-then-insert semantics to replace existing rows for the execution date.
   */

  def buildInsertSQL(p: Params, sourceTableEventPageChangeName: String): String ={
    s"""INSERT INTO ${p.target_table_metric} (
  dataset_date,
  tags,
  entity,
  instance,
  value,
  name,
  source_table,
  pipeline_run_id,
  partition_id,
  partition_ts
)
(
${buildSelectSQL(p,sourceTableEventPageChangeName)}
)"""
}

  /**
   * Returns a SELECT that reads alert rows from the metric table for the execution date.
   * Filters rows where the completeness ratio is below the alert threshold for the all-wikis aggregate.
   */
  def buildAlertSQL(p: Params): String ={
     s"""SELECT
                source_table,
                partition_id,
                partition_ts,
                'Success' AS status,
                'Error' AS severity_level,
                value,
                '${p.mode}' as constraint,
                null AS error_message,
                pipeline_run_id,
                dataset_date,
                tags
            FROM
                ${p.target_table_metric}
            WHERE
                value < ${p.alert_threshold}
                AND partition_ts = to_timestamp('${p.execution_date}')
                AND name = '${p.mode}'
                AND pipeline_run_id = '${p.airflow_run_id}'
                AND source_table = '${quotedTableName(p.source_table_event_page_change)}'
                AND tags.wiki_id = '${p.all_wiki_id}'
        """
  }

  /**
   * Writes the alert DataFrame as a plain-text file to HDFS.
   * Each row is formatted as a single line with tags, value, and threshold for human readability.
   */
  def exportAlert(p: Params, alertDf: DataFrame, spark: SparkSession): Unit ={

    val df = alertDf.select(
      expr(
        s"""concat('tags: ', string(tags), ', value: ', value, ', alerts_threshold: ', '${p.alert_threshold}')"""
    ).alias("alert"))

    val alertsText = df.collect().map(_.getAs[String]("alert")).mkString("\n")

    writeTextToHdfs(p.alert_hdfs_path, alertsText.trim, spark)
  }

  /**
   * Writes a text file to HDFS, replacing the file when it exists. It writes nothing when the text
   * is empty. A failed write is logged and does not fail the job, as HdfsTextWriter did.
   * Uses the Hadoop FileSystem API, so this module does not depend on the old refinery-spark module.
   */
  def writeTextToHdfs(outputPath: String, text: String, spark: SparkSession): Unit = {
    if (text.nonEmpty) {
      try {
        val path = new Path(outputPath)
        val fileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val out = fileSystem.create(path, true)
        try {
          out.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        } finally {
          out.close()
        }
      } catch {
        case e: java.io.IOException => log.error(s"Save operation failed with exception: $e")
      }
    }
  }

  /**
   * Pushes two Prometheus gauges to the Pushgateway:
   *   - _days: total distinct days with a metric row up to the execution date.
   *   - _alarms: total distinct days where the completeness ratio was below the threshold.
   * Both are recomputed from the durable metric table on each run, so the values survive job restarts.
   * It fails when there is no metric row up to the execution date, so a missing daily run is visible.
   */
  def exportMetricToPrometheus(p: Params, spark: SparkSession, sourceTableEventPageChangeName: String): Unit = {
    val registry = new CollectorRegistry()

    val alertsQuery =
      s"""SELECT count(distinct partition_ts) AS alert_count
         FROM ${p.target_table_metric}
         WHERE value < ${p.alert_threshold}
           AND name = '${p.mode}'
           AND source_table = '${quotedTableName(p.source_table_event_page_change)}'
           AND tags.wiki_id = '${p.all_wiki_id}'
           AND tags.project = '${sourceTableEventPageChangeName}_slo'
           AND partition_ts <= to_timestamp('${p.execution_date}')"""

    val metricsQuery =
      s"""SELECT count(distinct partition_ts) AS metrics_count
         FROM ${p.target_table_metric}
         WHERE name = '${p.mode}'
           AND source_table = '${quotedTableName(p.source_table_event_page_change)}'
           AND tags.wiki_id = '${p.all_wiki_id}'
           AND tags.project = '${sourceTableEventPageChangeName}_slo'
           AND partition_ts <= to_timestamp('${p.execution_date}')"""

    val alertsValue  = spark.sql(alertsQuery).collect().headOption.map(_.getAs[Long]("alert_count")).getOrElse(0L)
    val metricsValue = spark.sql(metricsQuery).collect().headOption.map(_.getAs[Long]("metrics_count")).getOrElse(0L)

    if (metricsValue > 0) {
      val metricDays = Gauge.build()
        .name(p.prometheus_metric + "_days")
        .help("Total days passed from the start of the metric measurement")
        .labelNames("wiki")
        .register(registry)

      val metricAlert = Gauge.build()
        .name(p.prometheus_metric + "_alarms")
        .help("Total number of days with an alarm since the start of the metric measurement")
        .labelNames("wiki")
        .register(registry)

      metricDays.labels(p.all_wiki_id).set(metricsValue.toDouble)
      metricAlert.labels(p.all_wiki_id).set(alertsValue.toDouble)

      val pushGateway = new PushGateway("prometheus-pushgateway.discovery.wmnet")
      pushGateway.push(registry, p.prometheus_metric)

      log.info(s"Pushed metrics to Prometheus Gateway - days: $metricsValue, alerts: $alertsValue")
    } else {
      throw new IllegalArgumentException(
        s"No metric rows found up to ${p.execution_date}; metrics are missing.")
    }
  }

}
