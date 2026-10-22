package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Tests for the pure / deterministic helpers of PageChangeCompletenessSlo.
 *
 * String-builder tests (parseArgs, generatePartitionId, buildSelectSQL,
 * buildInsertSQL, buildAlertSQL) run without Spark.
 * runCleanupSQL is exercised against a local Hadoop-catalog Iceberg table.
 *
 * main() and exportMetricToPrometheus (real PushGateway) are intentionally not
 * tested here. writeTextToHdfs is exercised against the local file system.
 */
class PageChangeCompletenessSloTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  import PageChangeCompletenessSlo.Params

  val warehouseDir: String =
    java.nio.file.Files.createTempDirectory("pagechange-slo-iceberg").toAbsolutePath.toString

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("PageChangeCompletenessSloTest")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions",              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local",           "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type",      "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouseDir)
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  /** Fully-populated Params; individual tests tweak fields via .copy(). */
  def fullParams: Params = Params(
    source_table_raw_revision     = "wmf_raw.mediawiki_revision_daily",
    source_table_raw_logging      = "wmf_raw.mediawiki_logging_daily",
    source_table_event_page_change = "event.mediawiki_page_change_v1",
    target_table_metric     = "wmf_data_ops.data_quality_metrics",
    target_table_alert      = "wmf_data_ops.data_quality_alerts",
    alert_hdfs_path         = "/tmp/alerts.txt",
    airflow_run_id         = "manual__2026-09-01",
    execution_date         = "2026-09-01",
    mode                  = "Completeness",
    alert_threshold        = "0.99",
    all_wiki_id             = "all-wikis",
    prometheus_metric      = "page_change_completeness"
  )

  // ---- parseArgs ----

  "PageChangeCompletenessSlo.parseArgs" should "populate every field from a full CLI arg array" in {
    val p = PageChangeCompletenessSlo.parseArgs(Array(
      "--source_table_raw_revision",     "wmf_raw.mediawiki_revision_daily",
      "--source_table_raw_logging",      "wmf_raw.mediawiki_logging_daily",
      "--source_table_event_page_change", "event.mediawiki_page_change_v1",
      "--target_table_metric",      "wmf_data_ops.data_quality_metrics",
      "--target_table_alert",       "wmf_data_ops.data_quality_alerts",
      "--alert_hdfs_path",          "/tmp/alerts.txt",
      "--airflow_run_id",           "manual__2026-09-01",
      "--execution_date",           "2026-09-01",
      "--mode",                     "Completeness",
      "--alert_threshold",          "0.99",
      "--all_wiki_id",              "all-wikis",
      "--prometheus_metric",        "page_change_completeness"
    ))
    p.source_table_raw_revision     shouldEqual "wmf_raw.mediawiki_revision_daily"
    p.source_table_raw_logging      shouldEqual "wmf_raw.mediawiki_logging_daily"
    p.source_table_event_page_change shouldEqual "event.mediawiki_page_change_v1"
    p.target_table_metric     shouldEqual "wmf_data_ops.data_quality_metrics"
    p.target_table_alert      shouldEqual "wmf_data_ops.data_quality_alerts"
    p.alert_hdfs_path         shouldEqual "/tmp/alerts.txt"
    p.airflow_run_id          shouldEqual "manual__2026-09-01"
    p.execution_date         shouldEqual "2026-09-01"
    p.mode                  shouldEqual "Completeness"
    p.alert_threshold        shouldEqual "0.99"
    p.all_wiki_id             shouldEqual "all-wikis"
    p.prometheus_metric      shouldEqual "page_change_completeness"
  }

  it should "default target_table_alert and alert_hdfs_path to an empty string when omitted" in {
    val p = PageChangeCompletenessSlo.parseArgs(Array(
      "--source_table_raw_revision",     "wmf_raw.mediawiki_revision_daily",
      "--source_table_raw_logging",      "wmf_raw.mediawiki_logging_daily",
      "--source_table_event_page_change", "event.mediawiki_page_change_v1",
      "--target_table_metric",      "wmf_data_ops.data_quality_metrics",
      "--airflow_run_id",           "manual__2026-09-01",
      "--execution_date",           "2026-09-01",
      "--mode",                     "Completeness",
      "--alert_threshold",          "0.99",
      "--all_wiki_id",              "all-wikis",
      "--prometheus_metric",        "page_change_completeness"
    ))
    p.target_table_alert shouldEqual ""
    p.alert_hdfs_path    shouldEqual ""
    // Required fields still populated.
    p.source_table_raw_revision     shouldEqual "wmf_raw.mediawiki_revision_daily"
    p.source_table_raw_logging      shouldEqual "wmf_raw.mediawiki_logging_daily"
    p.execution_date              shouldEqual "2026-09-01"
    p.source_table_event_page_change shouldEqual "event.mediawiki_page_change_v1"
    p.target_table_metric     shouldEqual "wmf_data_ops.data_quality_metrics"
    p.prometheus_metric      shouldEqual "page_change_completeness"
  }

  it should "fail and name the argument when a required argument is missing" in {
    val e = intercept[Exception] {
      PageChangeCompletenessSlo.parseArgs(Array(
        "--source_table_raw_revision",     "wmf_raw.mediawiki_revision_daily",
        "--source_table_raw_logging",      "wmf_raw.mediawiki_logging_daily",
        "--source_table_event_page_change", "event.mediawiki_page_change_v1",
        "--target_table_metric",      "wmf_data_ops.data_quality_metrics",
        "--airflow_run_id",           "manual__2026-09-01",
        "--execution_date",           "2026-09-01",
        "--alert_threshold",          "0.99",
        "--all_wiki_id",              "all-wikis",
        "--prometheus_metric",        "page_change_completeness"
      ))
    }
    e.getMessage should include ("mode")
  }

  // ---- generatePartitionId ----

  "PageChangeCompletenessSlo.generatePartitionId" should "emit a concat expression with non-zero-padded month and day" in {
    val id = PageChangeCompletenessSlo.generatePartitionId("2026-09-01")
    id shouldEqual "concat('wiki_id=',wiki_id,'/year=2026/month=9/day=1')"
  }

  it should "not zero-pad a two-digit month and day" in {
    val id = PageChangeCompletenessSlo.generatePartitionId("2026-12-25")
    id shouldEqual "concat('wiki_id=',wiki_id,'/year=2026/month=12/day=25')"
  }

  it should "resolve wiki_id to the value of the column in each row when run in Spark" in {
    import spark.implicits._
    Seq("enwiki", "all-wikis").toDF("wiki_id").createOrReplaceTempView("partition_id_wikis")
    val expr = PageChangeCompletenessSlo.generatePartitionId("2026-09-01")
    val ids = spark.sql(s"SELECT $expr AS partition_id FROM partition_id_wikis")
      .collect().map(_.getString(0)).toSet
    ids shouldEqual Set(
      "wiki_id=enwiki/year=2026/month=9/day=1",
      "wiki_id=all-wikis/year=2026/month=9/day=1"
    )
  }

  // ---- quotedTableName ----

  "PageChangeCompletenessSlo.quotedTableName" should "quote the schema and the table with backticks" in {
    PageChangeCompletenessSlo.quotedTableName("wmf_content.mediawiki_content_history_v1") shouldEqual
      "`wmf_content`.`mediawiki_content_history_v1`"
  }

  it should "leave a name that is already quoted unchanged" in {
    PageChangeCompletenessSlo.quotedTableName("`wmf_content`.`mediawiki_content_history_v1`") shouldEqual
      "`wmf_content`.`mediawiki_content_history_v1`"
  }

  // ---- buildSelectSQL ----

  "PageChangeCompletenessSlo.buildSelectSQL" should "filter events on the year, month and day partition of the execution date" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("year=2026")
    sql should include ("month=9")
    sql should include ("day=1")
  }

  it should "read the daily revision and logging tables passed as parameters" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("wmf_raw.mediawiki_revision_daily")
    sql should include ("wmf_raw.mediawiki_logging_daily")
  }

  it should "filter the daily tables on the day partition and on the event timestamp of the execution date" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    "\\n\\s+day = '2026-09-01'".r.findAllMatchIn(sql).size shouldEqual 2
    sql should include ("TO_DATE(rev_timestamp, 'yyyyMMddHHmmss') = '2026-09-01'")
    sql should include ("TO_DATE(log_timestamp, 'yyyyMMddHHmmss') = '2026-09-01'")
  }

  it should "read only page delete and restore entries from the logging table" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("log_type = 'delete'")
    sql should include ("log_action in ('delete', 'restore')")
  }

  it should "measure the execution date and store the rows under the same date" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams.copy(execution_date = "2026-08-21"), "page_change")
    sql should include ("month=8")
    sql should include ("day=21")
    sql should include ("'2026-08-21' as revision_date")
    sql should include ("to_timestamp('2026-08-21') as partition_ts")
  }

  it should "tag rows with the project name derived from the page-change table name" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("'project', 'page_change_slo'")
  }

  it should "tag rows with the measured day and the missing and total counts of every change type" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("'2026-09-01' as revision_date")
    for (tag <- Seq("max_timestamp", "missing_events", "missing_revisions", "missing_deletes",
                    "missing_restores", "total_count", "revision_count", "delete_count", "restore_count")) {
      sql should include (s"'$tag',")
    }
    sql should not include ("'change_type',")
  }

  it should "set the metric name to the mode and carry source_table and pipeline_run_id" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("'Completeness' as name")
    sql should include ("'`event`.`mediawiki_page_change_v1`' AS source_table")
    sql should include ("'manual__2026-09-01' as pipeline_run_id")
  }

  it should "embed the generatePartitionId expression" in {
    val expectedPartitionId =
      PageChangeCompletenessSlo.generatePartitionId(fullParams.execution_date)
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include (s"$expectedPartitionId as partition_id")
  }

  it should "deduplicate the events and revision snapshot CTEs with distinct" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("distinct revision.rev_id as revision_id")
    sql should include ("distinct rev_id as revision_id")
  }

  it should "match deletes and restores on page_id and change_type, and revisions on revision_id" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    sql should include ("left anti join")
    sql should include ("and sd.page_id = e.page_id")
    sql should include ("and sd.revision_id = e.revision_id")
    // Both anti-joins match on change_type.
    "and sd.change_type = e.change_type".r.findAllMatchIn(sql).size shouldEqual 2
  }

  it should "produce one row per wiki and one all-wikis row, with the counts of every change type" in {
    val sql = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    "'all-wikis' as wiki_id".r.findAllMatchIn(sql).size shouldEqual 1
    sql should include ("when change_type = 'revision'")
    sql should include ("when change_type = 'delete'")
    sql should include ("when change_type = 'restore'")
    // snapshot_data, missing_counts and the all-wikis row of total.
    "union all".r.findAllMatchIn(sql).size shouldEqual 3
  }

  // ---- buildInsertSQL ----

  "PageChangeCompletenessSlo.buildInsertSQL" should "start with INSERT INTO the metric table and list the target columns" in {
    val sql = PageChangeCompletenessSlo.buildInsertSQL(fullParams, "page_change")
    sql should startWith ("INSERT INTO wmf_data_ops.data_quality_metrics (")
    for (col <- Seq("dataset_date", "tags", "entity", "instance", "value",
                    "name", "source_table", "pipeline_run_id", "partition_id", "partition_ts")) {
      sql should include (col)
    }
  }

  it should "embed the buildSelectSQL body" in {
    // dataset_date holds the current time in milliseconds, which differs between two
    // calls, so compare the SQL without that line.
    def withoutDatasetDate(sql: String): String =
      sql.split("\n").filterNot(_.contains("as dataset_date")).mkString("\n")
    val select = PageChangeCompletenessSlo.buildSelectSQL(fullParams, "page_change")
    val insert = PageChangeCompletenessSlo.buildInsertSQL(fullParams, "page_change")
    withoutDatasetDate(insert) should include (withoutDatasetDate(select))
  }

  // ---- buildAlertSQL ----

  "PageChangeCompletenessSlo.buildAlertSQL" should "select from the metric table" in {
    val sql = PageChangeCompletenessSlo.buildAlertSQL(fullParams)
    sql should include ("FROM\n                wmf_data_ops.data_quality_metrics")
  }

  it should "filter on threshold, partition_ts, name, pipeline_run_id, source_table and all-wiki id" in {
    val sql = PageChangeCompletenessSlo.buildAlertSQL(fullParams)
    sql should include ("value < 0.99")
    sql should include ("partition_ts = to_timestamp('2026-09-01')")
    sql should include ("name = 'Completeness'")
    sql should include ("pipeline_run_id = 'manual__2026-09-01'")
    sql should include ("source_table = '`event`.`mediawiki_page_change_v1`'")
    sql should include ("tags.wiki_id = 'all-wikis'")
  }

  it should "set the constraint column to the mode" in {
    val sql = PageChangeCompletenessSlo.buildAlertSQL(fullParams)
    sql should include ("'Completeness' as constraint")
  }

  // ---- runCleanupSQL (Iceberg) ----

  "PageChangeCompletenessSlo.runCleanupSQL" should "delete only the matching metric and alert rows" in {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.dq_metrics")
    spark.sql("DROP TABLE IF EXISTS local.db.dq_alerts")

    spark.sql(
      """CREATE TABLE local.db.dq_metrics (
           partition_ts TIMESTAMP,
           source_table STRING,
           name         STRING,
           tags         STRUCT<project: STRING>
         ) USING iceberg"""
    )
    spark.sql(
      """CREATE TABLE local.db.dq_alerts (
           partition_ts TIMESTAMP,
           source_table STRING,
           `constraint` STRING,
           tags         STRUCT<project: STRING>
         ) USING iceberg"""
    )

    // Metrics: one row matches the cleanup predicate exactly, two differ on one field each.
    spark.sql(
      """INSERT INTO local.db.dq_metrics VALUES
           (TIMESTAMP '2026-09-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Completeness', named_struct('project', 'page_change_slo')),
           (TIMESTAMP '2026-09-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Completeness', named_struct('project', 'other_slo')),
           (TIMESTAMP '2026-10-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Completeness', named_struct('project', 'page_change_slo'))"""
    )
    spark.sql(
      """INSERT INTO local.db.dq_alerts VALUES
           (TIMESTAMP '2026-09-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Completeness', named_struct('project', 'page_change_slo')),
           (TIMESTAMP '2026-09-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Accuracy', named_struct('project', 'page_change_slo'))"""
    )

    val p = fullParams.copy(
      target_table_metric = "local.db.dq_metrics",
      target_table_alert  = "local.db.dq_alerts"
    )
    PageChangeCompletenessSlo.runCleanupSQL(p, spark, "page_change")

    // Metrics: only the non-matching project and the future partition survive.
    val metricRows = spark.sql(
      "SELECT tags.project AS project, date_format(partition_ts, 'yyyy-MM-dd') AS d FROM local.db.dq_metrics"
    ).collect().map(r => (r.getString(0), r.getString(1))).toSet
    metricRows shouldEqual Set(("other_slo", "2026-09-01"), ("page_change_slo", "2026-10-01"))

    // Alerts: only the Accuracy row survives (the Completeness one is cleaned).
    val alertRows = spark.sql("SELECT `constraint` FROM local.db.dq_alerts")
      .collect().map(_.getString(0)).toSet
    alertRows shouldEqual Set("Accuracy")
  }

  it should "clean only the metric rows when there is no alert table" in {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.dq_metrics_no_alert")
    spark.sql(
      """CREATE TABLE local.db.dq_metrics_no_alert (
           partition_ts TIMESTAMP,
           source_table STRING,
           name         STRING,
           tags         STRUCT<project: STRING>
         ) USING iceberg"""
    )
    spark.sql(
      """INSERT INTO local.db.dq_metrics_no_alert VALUES
           (TIMESTAMP '2026-09-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Completeness', named_struct('project', 'page_change_slo'))"""
    )

    val p = fullParams.copy(target_table_metric = "local.db.dq_metrics_no_alert", target_table_alert = "")
    PageChangeCompletenessSlo.runCleanupSQL(p, spark, "page_change")

    spark.table("local.db.dq_metrics_no_alert").count() shouldEqual 0
  }

  // ---- checkSnapshotNotEmpty (Iceberg) ----

  def createMetricsTableWithAllWikisTotal(table: String, total: String): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(
      s"""CREATE TABLE $table (
           partition_ts    TIMESTAMP,
           source_table    STRING,
           name            STRING,
           pipeline_run_id STRING,
           tags            MAP<STRING, STRING>
         ) USING iceberg"""
    )
    spark.sql(
      s"""INSERT INTO $table VALUES
           (TIMESTAMP '2026-09-01 00:00:00', '`event`.`mediawiki_page_change_v1`', 'Completeness', 'manual__2026-09-01',
            map('project', 'page_change_slo', 'wiki_id', 'all-wikis', 'total_count', $total))"""
    )
  }

  "PageChangeCompletenessSlo.checkSnapshotNotEmpty" should "fail when the snapshot has no rows" in {
    createMetricsTableWithAllWikisTotal("local.db.dq_metrics_empty", "CAST(NULL AS STRING)")
    val p = fullParams.copy(target_table_metric = "local.db.dq_metrics_empty")
    an [IllegalStateException] should be thrownBy
      PageChangeCompletenessSlo.checkSnapshotNotEmpty(p, spark, "page_change")
  }

  it should "pass when the snapshot has rows" in {
    createMetricsTableWithAllWikisTotal("local.db.dq_metrics_full", "'120'")
    val p = fullParams.copy(target_table_metric = "local.db.dq_metrics_full")
    PageChangeCompletenessSlo.checkSnapshotNotEmpty(p, spark, "page_change")
  }

  // ---- writeTextToHdfs (local file system) ----

  "PageChangeCompletenessSlo.writeTextToHdfs" should "write the text to the path and replace an existing file" in {
    val dir  = java.nio.file.Files.createTempDirectory("pagechange-slo-alerts").toAbsolutePath.toString
    val path = s"$dir/alerts.txt"

    PageChangeCompletenessSlo.writeTextToHdfs(path, "first alert", spark)
    PageChangeCompletenessSlo.writeTextToHdfs(path, "line 1\nline 2", spark)

    val content = scala.io.Source.fromFile(path, "UTF-8").mkString
    content shouldEqual "line 1\nline 2"
  }

  it should "write nothing when the text is empty" in {
    val dir  = java.nio.file.Files.createTempDirectory("pagechange-slo-alerts").toAbsolutePath.toString
    val path = s"$dir/alerts.txt"

    PageChangeCompletenessSlo.writeTextToHdfs(path, "", spark)

    new java.io.File(path).exists shouldBe false
  }
}
