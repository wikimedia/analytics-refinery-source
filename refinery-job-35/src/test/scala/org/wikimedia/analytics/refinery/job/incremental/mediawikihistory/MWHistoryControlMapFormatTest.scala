package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Verifies that every control_map timestamp value written by MWHistoryDeltaWriter
 * uses the same ISO-8601 UTC format (yyyy-MM-dd'T'HH:mm:ss.SSS'Z').
 *
 * Two input types are in play across the nine write sites:
 *   TIMESTAMP — meta_dt produced by to_timestamp(meta.dt); used by M1, M2, M5, M6, M7.
 *   STRING    — meta.dt passed as-is from the event; used by M3 (tags), M4 (visibility).
 *
 * Expression tests verify the date_format pattern for both types without Iceberg.
 * The integration test runs an actual MERGE against an Iceberg table and reads back
 * the written value, confirming the format survives the full SQL execution path.
 */
class MWHistoryControlMapFormatTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val warehouseDir: String =
    java.nio.file.Files.createTempDirectory("mwh-cm-format-iceberg").toAbsolutePath.toString

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MWHistoryControlMapFormatTest")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions",             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local",          "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type",     "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouseDir)
    .getOrCreate()

  val iso8601: scala.util.matching.Regex = """^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$""".r

  override def beforeAll(): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS local.db.cm_format_target (
           wiki_id         STRING,
           revision_id     BIGINT,
           event_timestamp TIMESTAMP,
           revision_tags   ARRAY<STRING>,
           control_map     MAP<STRING,STRING>
         ) USING iceberg"""
    )
    spark.sql(
      """INSERT INTO local.db.cm_format_target VALUES (
           'enwiki', 101L, TIMESTAMP '2024-01-15 10:00:00',
           CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS MAP<STRING,STRING>)
         )"""
    )
  }

  override def afterAll(): Unit = spark.stop()

  // --- Expression tests (no Iceberg) ---

  "control_map date_format expression" should "produce ISO-8601 UTC from a TIMESTAMP input (M1/M2/M5/M6/M7 path)" in {
    val result = spark.sql(
      """SELECT date_format(to_timestamp('2024-01-15T10:00:01.370Z'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS dt"""
    ).collect()(0).getString(0)
    result shouldEqual "2024-01-15T10:00:01.370Z"
  }

  it should "produce ISO-8601 UTC from a raw ISO-8601 STRING input (M3/M4 path)" in {
    val result = spark.sql(
      """SELECT date_format('2024-01-15T10:00:01.370Z', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS dt"""
    ).collect()(0).getString(0)
    result shouldEqual "2024-01-15T10:00:01.370Z"
  }

  // --- Integration test (Iceberg MERGE) ---

  "tags_update_dt written by buildTagsMergeSQL" should "match ISO-8601 UTC in the Iceberg table" in {
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_tags AS
          SELECT
            t.wiki_id  AS database,
            t.rev_id   AS rev_id,
            t.tags     AS tags,
            named_struct('dt', t.meta_dt) AS meta,
            2024 AS year, 1 AS month, 15 AS day
          FROM (SELECT * FROM VALUES ('enwiki', 101L, array('wikify'), '2024-01-15T10:00:01.370Z'))
            AS t(wiki_id, rev_id, tags, meta_dt)"""
    )
    val p = MWHistoryDeltaWriter.Params(
      tagsTable   = "test_tags",
      targetTable = "db.cm_format_target",
      catalog     = "local",
      year = 2024, month = 1, day = 15
    )
    spark.sql(MWHistoryDeltaWriter.buildTagsMergeSQL(p))
    val dt = spark.sql(
      "SELECT control_map['tags_update_dt'] FROM local.db.cm_format_target WHERE wiki_id = 'enwiki' AND revision_id = 101"
    ).collect()(0).getString(0)
    dt should fullyMatch regex iso8601
    dt shouldEqual "2024-01-15T10:00:01.370Z"
  }
}
