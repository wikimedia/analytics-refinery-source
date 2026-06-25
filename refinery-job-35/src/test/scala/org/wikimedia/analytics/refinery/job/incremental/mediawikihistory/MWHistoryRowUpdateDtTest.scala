package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Tests for the row_update_dt watermark column written by MWHistoryDeltaWriter.
 *
 * row_update_dt = the Airflow data_interval_end, derived from the run day as day + 1 at UTC
 * midnight (Params.rowUpdateDt). Daily MERGEs stamp it with GREATEST(existing, literal) on
 * UPDATE so an out-of-order backfill can never lower a row's watermark below a value consumers
 * have already passed, and so a reconcile-NULL'd row is lifted to the literal. The Iceberg tests
 * exercise this through MERGE 3 (buildTagsMergeSQL), the simplest UPDATE-only MERGE; the
 * GREATEST/literal logic is identical across all daily write sites.
 */
class MWHistoryRowUpdateDtTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val warehouseDir: String =
    java.nio.file.Files.createTempDirectory("mwh-row-update-dt-iceberg").toAbsolutePath.toString

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MWHistoryRowUpdateDtTest")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions",             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local",          "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type",     "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouseDir)
    .getOrCreate()

  override def beforeAll(): Unit = {
    // Second-and-later MERGEs map_concat a key already present in control_map; LAST_WIN avoids
    // DUPLICATED_MAP_KEY (matches MWHistoryDeltaWriter.run).
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
  }

  override def afterAll(): Unit = spark.stop()

  /** Fresh single-row target whose row_update_dt starts NULL, as a reconcile (snapshot) row would. */
  def resetTarget(eventTimestamp: String): Unit = {
    spark.sql("DROP TABLE IF EXISTS local.db.rud_target")
    spark.sql(
      """CREATE TABLE local.db.rud_target (
           wiki_id         STRING,
           revision_id     BIGINT,
           event_timestamp TIMESTAMP,
           revision_tags   ARRAY<STRING>,
           control_map     MAP<STRING,STRING>,
           row_update_dt   TIMESTAMP
         ) USING iceberg"""
    )
    spark.sql(
      s"""INSERT INTO local.db.rud_target VALUES (
           'enwiki', 101L, TIMESTAMP '$eventTimestamp',
           CAST(NULL AS ARRAY<STRING>), CAST(NULL AS MAP<STRING,STRING>), CAST(NULL AS TIMESTAMP)
         )"""
    )
  }

  /** Runs MERGE 3 (tags) for the given run day against the single target row. */
  def runTagsMerge(year: Int, month: Int, day: Int): Unit = {
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_tags AS
          SELECT 'enwiki' AS database, 101L AS rev_id, array('wikify') AS tags,
                 named_struct('dt', '2024-01-15T10:00:01.370Z') AS meta,
                 $year AS year, $month AS month, $day AS day"""
    )
    val p = MWHistoryDeltaWriter.Params(
      tagsTable = "test_tags", targetTable = "db.rud_target", catalog = "local",
      year = year, month = month, day = day
    )
    spark.sql(MWHistoryDeltaRevisionSQL.buildRevisionTagsMergeSQL(p))
  }

  // Format Spark-side in the (UTC) session timezone. java.sql.Timestamp.toString renders in the
  // JVM-local zone, which would misreport the UTC value on a non-UTC machine.
  def rowUpdateDt(): String =
    spark.sql("SELECT date_format(row_update_dt, 'yyyy-MM-dd HH:mm:ss') AS d FROM local.db.rud_target WHERE revision_id = 101")
      .collect()(0).getString(0)

  // --- Params.rowUpdateDt derivation (no Spark) ---

  "Params.rowUpdateDt" should "derive data_interval_end as run day + 1 at UTC midnight" in {
    MWHistoryDeltaWriter.Params(year = 2024, month = 1, day = 15).rowUpdateDt shouldEqual "2024-01-16 00:00:00"
  }

  it should "roll over month and year boundaries" in {
    MWHistoryDeltaWriter.Params(year = 2024, month = 12, day = 31).rowUpdateDt shouldEqual "2025-01-01 00:00:00"
  }

  // --- Iceberg MERGE (GREATEST semantics) ---

  "A daily MERGE" should "stamp row_update_dt = day+1 on a row whose value was NULL" in {
    resetTarget("2024-01-15 10:00:00")
    runTagsMerge(2024, 1, 15)
    rowUpdateDt() should startWith ("2024-01-16 00:00:00")
  }

  it should "advance row_update_dt when a later run touches the row" in {
    resetTarget("2024-01-15 10:00:00")
    runTagsMerge(2024, 1, 15)
    runTagsMerge(2024, 2, 20)
    rowUpdateDt() should startWith ("2024-02-21 00:00:00")
  }

  it should "not lower row_update_dt when an out-of-order earlier run touches the row" in {
    resetTarget("2024-01-15 10:00:00")
    runTagsMerge(2024, 2, 20)
    rowUpdateDt() should startWith ("2024-02-21 00:00:00")
    runTagsMerge(2024, 1, 5)                                 // backfill of an earlier logical date
    rowUpdateDt() should startWith ("2024-02-21 00:00:00")   // GREATEST guard: unchanged
  }
}
