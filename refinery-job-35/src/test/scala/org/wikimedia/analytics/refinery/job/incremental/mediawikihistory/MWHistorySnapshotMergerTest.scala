package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Tests for MWHistorySnapshotMerger.
 *
 * Projection tests run against a synthetic temp view, without Iceberg.
 * MERGE tests run against a local Hadoop-catalog Iceberg table to verify the
 * three-clause merge behavior, including the event-row cleanup timestamp bounds.
 */
class MWHistorySnapshotMergerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val warehouseDir: String =
    java.nio.file.Files.createTempDirectory("mwh-merger-iceberg").toAbsolutePath.toString

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MWHistorySnapshotMergerTest")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions",         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local",      "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouseDir)
    .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS local.db.test_target (
           source                                                        STRING,
           wiki_id                                                       STRING,
           event_entity                                                  STRING,
           event_type                                                    STRING,
           event_timestamp                                               TIMESTAMP,
           event_user_id                                                 BIGINT,
           event_user_central_id                                         BIGINT,
           event_user_text_historical                                    STRING,
           event_user_is_anonymous                                       BOOLEAN,
           event_user_is_temporary                                       BOOLEAN,
           event_user_is_permanent                                       BOOLEAN,
           event_user_registration_timestamp                             TIMESTAMP,
           event_user_is_created_by_self                                 BOOLEAN,
           page_id                                                       BIGINT,
           page_title_historical                                         STRING,
           page_namespace_historical                                     INT,
           revision_id                                                   BIGINT,
           revision_parent_id                                            BIGINT,
           revision_minor_edit                                           BOOLEAN,
           revision_text_bytes                                           BIGINT,
           revision_text_bytes_diff                                      BIGINT,
           revision_text_sha1                                            STRING,
           revision_tags                                                 ARRAY<STRING>,
           page_namespace_is_content_historical                          BOOLEAN,
           event_user_is_bot_by_historical                               ARRAY<STRING>,
           revision_deleted_parts                                        ARRAY<STRING>,
           event_user_revision_count                                     BIGINT,
           event_user_groups_historical                                  ARRAY<STRING>,
           user_id                                                       BIGINT,
           user_text_historical                                          STRING,
           user_is_anonymous                                             BOOLEAN,
           user_is_temporary                                             BOOLEAN,
           user_is_permanent                                             BOOLEAN,
           user_groups_historical                                        ARRAY<STRING>,
           user_is_bot_by_historical                                     ARRAY<STRING>,
           user_is_created_by_self                                       BOOLEAN,
           user_is_created_by_system                                     BOOLEAN,
           user_is_created_by_peer                                       BOOLEAN,
           revision_is_identity_reverted                                 BOOLEAN,
           revision_first_identity_reverting_revision_id                 BIGINT,
           revision_seconds_to_identity_revert                           BIGINT,
           revision_is_identity_revert                                   BOOLEAN,
           revision_is_identity_reverted_within_90_days                  BOOLEAN,
           revision_first_identity_reverting_revision_id_within_90_days  BIGINT,
           revision_seconds_to_identity_revert_within_90_days            BIGINT,
           revision_is_identity_revert_within_90_days                    BOOLEAN
         ) USING iceberg
         PARTITIONED BY (source, days(event_timestamp))"""
    )
  }

  override def afterAll(): Unit = spark.stop()

  /** Inserts a minimal source='events' row into the Iceberg target at the given timestamp. */
  def insertEventsRow(revisionId: Long, eventTimestamp: String): Unit =
    spark.sql(
      s"""INSERT INTO local.db.test_target VALUES (
           'events', 'enwiki', 'revision', 'edit',
           TIMESTAMP '$eventTimestamp',
           CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS STRING), false, false, false,
           CAST(NULL AS TIMESTAMP), false,
           CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(0 AS INT),
           CAST($revisionId AS BIGINT), CAST(NULL AS BIGINT), false,
           CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS STRING),
           CAST(NULL AS ARRAY<STRING>), false, CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS BIGINT),
           CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS BIGINT), CAST(NULL AS STRING),
           CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN),
           CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN),
           CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BOOLEAN),
           false, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), false
         )"""
    )

  val params: MWHistorySnapshotMerger.Params = MWHistorySnapshotMerger.Params(
    sourceTable = "test_mwh",
    targetTable = "test_target",
    snapshot    = "2024-01"
  )

  val mergeParams: MWHistorySnapshotMerger.Params = MWHistorySnapshotMerger.Params(
    sourceTable = "test_mwh",
    targetTable = "local.db.test_target",
    snapshot    = "2024-01"
  )

  def projected() = spark.sql(MWHistorySnapshotMerger.buildProjectionSQL(params) + "\nSELECT * FROM projected_monthly")

  /** One row matching the relevant columns of wmf.mediawiki_history. */
  def registerSource(): Unit =
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT
           'enwiki'                          AS wiki_db,
           'revision'                        AS event_entity,
           'edit'                            AS event_type,
           '2024-01-15 10:00:00.0'            AS event_timestamp,
           CAST(42 AS BIGINT)                AS event_user_id,
           CAST(NULL AS BIGINT)              AS event_user_central_id,
           'Alice'                           AS event_user_text_historical,
           false                             AS event_user_is_anonymous,
           false                             AS event_user_is_temporary,
           true                              AS event_user_is_permanent,
           '2000-01-01 00:00:00.0'           AS event_user_registration_timestamp,
           true                              AS event_user_is_created_by_self,
           CAST(1 AS BIGINT)                 AS page_id,
           'Main_Page'                       AS page_title_historical,
           CAST(0 AS INT)                    AS page_namespace_historical,
           CAST(101 AS BIGINT)               AS revision_id,
           CAST(100 AS BIGINT)               AS revision_parent_id,
           false                             AS revision_minor_edit,
           CAST(500 AS BIGINT)               AS revision_text_bytes,
           CAST(200 AS BIGINT)               AS revision_text_bytes_diff,
           'abc123'                          AS revision_text_sha1,
           array('tag1')                     AS revision_tags,
           true                              AS page_namespace_is_content_historical,
           array('name')                     AS event_user_is_bot_by_historical,
           CAST(NULL AS ARRAY<STRING>)       AS revision_deleted_parts,
           CAST(7 AS BIGINT)                 AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
           false                             AS revision_is_identity_reverted,
           CAST(NULL AS BIGINT)              AS revision_first_identity_reverting_revision_id,
           CAST(NULL AS BIGINT)              AS revision_seconds_to_identity_revert,
           false                             AS revision_is_identity_revert,
           '2024-01'                         AS snapshot"""
    )

  // ---- Argument parsing ----

  "MWHistorySnapshotMerger.parseArgs" should "map CLI flags to Params fields" in {
    val p = MWHistorySnapshotMerger.parseArgs(Array(
      "--source_table", "wmf.mediawiki_history",
      "--target_table", "analytics.mediawiki_history_incremental_v1",
      "--snapshot",     "2024-01"
    ))
    p.sourceTable shouldEqual "wmf.mediawiki_history"
    p.targetTable shouldEqual "analytics.mediawiki_history_incremental_v1"
    p.snapshot    shouldEqual "2024-01"
  }

  // ---- Projection ----

  "MWHistorySnapshotMerger" should "set source to 'snapshot'" in {
    registerSource()
    projected().collect()(0).getAs[String]("source") shouldEqual "snapshot"
  }

  it should "pass through scalar columns unchanged" in {
    registerSource()
    val row = projected().collect()(0)
    row.getAs[String]("wiki_id")                    shouldEqual "enwiki"
    row.getAs[String]("event_entity")               shouldEqual "revision"
    row.getAs[String]("event_type")                 shouldEqual "edit"
    row.getAs[Long]("event_user_id")                shouldEqual 42L
    row.getAs[String]("event_user_text_historical") shouldEqual "Alice"
    row.getAs[Boolean]("event_user_is_anonymous")   shouldEqual false
    row.getAs[Boolean]("event_user_is_permanent")   shouldEqual true
    row.getAs[Boolean]("event_user_is_created_by_self") shouldEqual true
    row.getAs[Long]("revision_id")                  shouldEqual 101L
    row.getAs[Long]("revision_text_bytes")          shouldEqual 500L
    row.getAs[Long]("revision_text_bytes_diff")     shouldEqual 200L
    row.getAs[String]("revision_text_sha1")         shouldEqual "abc123"
    row.getAs[Long]("event_user_revision_count")    shouldEqual 7L
  }

  it should "cast event_timestamp from mediawiki string format to TIMESTAMP" in {
    registerSource()
    val ts = projected().collect()(0).getAs[java.sql.Timestamp]("event_timestamp")
    ts should not be null
    ts.toString should startWith("2024-01-15 10:00:00")
  }

  it should "cast event_user_registration_timestamp from mediawiki string format to TIMESTAMP" in {
    registerSource()
    val ts = projected().collect()(0).getAs[java.sql.Timestamp]("event_user_registration_timestamp")
    ts should not be null
    ts.toString should startWith("2000-01-01")
  }

  it should "project false authoritative revert fields when revision is not reverted" in {
    registerSource()
    val row = projected().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted") shouldEqual false
    row.getAs[Boolean]("revision_is_identity_revert")   shouldEqual false
  }

  it should "set bounded revert fields to false when revision is not reverted" in {
    registerSource()
    val row = projected().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted_within_90_days") shouldEqual false
    row.getAs[Boolean]("revision_is_identity_revert_within_90_days")   shouldEqual false
    row.getAs[java.lang.Long]("revision_first_identity_reverting_revision_id_within_90_days") shouldBe null
    row.getAs[java.lang.Long]("revision_seconds_to_identity_revert_within_90_days")           shouldBe null
  }

  it should "set revision_is_identity_reverted_within_90_days true when reverted within 90d" in {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT
           'enwiki' AS wiki_db, 'revision' AS event_entity, 'edit' AS event_type,
           '2024-01-15 10:00:00.0' AS event_timestamp,
           CAST(NULL AS BIGINT) AS event_user_id, CAST(NULL AS BIGINT) AS event_user_central_id,
           CAST(NULL AS STRING) AS event_user_text_historical,
           CAST(NULL AS BOOLEAN) AS event_user_is_anonymous,
           CAST(NULL AS BOOLEAN) AS event_user_is_temporary,
           CAST(NULL AS BOOLEAN) AS event_user_is_permanent,
           CAST(NULL AS STRING) AS event_user_registration_timestamp,
           CAST(NULL AS BOOLEAN) AS event_user_is_created_by_self,
           CAST(1 AS BIGINT) AS page_id, CAST(NULL AS STRING) AS page_title_historical,
           CAST(0 AS INT) AS page_namespace_historical,
           CAST(101 AS BIGINT) AS revision_id, CAST(NULL AS BIGINT) AS revision_parent_id,
           CAST(NULL AS BOOLEAN) AS revision_minor_edit,
           CAST(NULL AS BIGINT) AS revision_text_bytes, CAST(NULL AS BIGINT) AS revision_text_bytes_diff,
           CAST(NULL AS STRING) AS revision_text_sha1, CAST(NULL AS ARRAY<STRING>) AS revision_tags,
           CAST(NULL AS BOOLEAN) AS page_namespace_is_content_historical,
           CAST(NULL AS ARRAY<STRING>) AS event_user_is_bot_by_historical,
           CAST(NULL AS ARRAY<STRING>) AS revision_deleted_parts,
           CAST(NULL AS BIGINT) AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
           true                    AS revision_is_identity_reverted,
           CAST(200 AS BIGINT)     AS revision_first_identity_reverting_revision_id,
           CAST(3600 AS BIGINT)    AS revision_seconds_to_identity_revert,
           false                   AS revision_is_identity_revert,
           '2024-01' AS snapshot"""
    )

    val row = projected().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted_within_90_days")               shouldEqual true
    row.getAs[Long]("revision_first_identity_reverting_revision_id_within_90_days")  shouldEqual 200L
    row.getAs[Long]("revision_seconds_to_identity_revert_within_90_days")            shouldEqual 3600L
  }

  it should "set revision_is_identity_reverted_within_90_days false when reverted outside 90d" in {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT
           'enwiki' AS wiki_db, 'revision' AS event_entity, 'edit' AS event_type,
           '2024-01-15 10:00:00.0' AS event_timestamp,
           CAST(NULL AS BIGINT) AS event_user_id, CAST(NULL AS BIGINT) AS event_user_central_id,
           CAST(NULL AS STRING) AS event_user_text_historical,
           CAST(NULL AS BOOLEAN) AS event_user_is_anonymous,
           CAST(NULL AS BOOLEAN) AS event_user_is_temporary,
           CAST(NULL AS BOOLEAN) AS event_user_is_permanent,
           CAST(NULL AS STRING) AS event_user_registration_timestamp,
           CAST(NULL AS BOOLEAN) AS event_user_is_created_by_self,
           CAST(1 AS BIGINT) AS page_id, CAST(NULL AS STRING) AS page_title_historical,
           CAST(0 AS INT) AS page_namespace_historical,
           CAST(101 AS BIGINT) AS revision_id, CAST(NULL AS BIGINT) AS revision_parent_id,
           CAST(NULL AS BOOLEAN) AS revision_minor_edit,
           CAST(NULL AS BIGINT) AS revision_text_bytes, CAST(NULL AS BIGINT) AS revision_text_bytes_diff,
           CAST(NULL AS STRING) AS revision_text_sha1, CAST(NULL AS ARRAY<STRING>) AS revision_tags,
           CAST(NULL AS BOOLEAN) AS page_namespace_is_content_historical,
           CAST(NULL AS ARRAY<STRING>) AS event_user_is_bot_by_historical,
           CAST(NULL AS ARRAY<STRING>) AS revision_deleted_parts,
           CAST(NULL AS BIGINT) AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
           true                         AS revision_is_identity_reverted,
           CAST(200 AS BIGINT)          AS revision_first_identity_reverting_revision_id,
           CAST(91 * 86400 AS BIGINT)   AS revision_seconds_to_identity_revert,
           false                        AS revision_is_identity_revert,
           '2024-01' AS snapshot"""
    )

    val row = projected().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted_within_90_days")               shouldEqual false
    row.getAs[java.lang.Long]("revision_first_identity_reverting_revision_id_within_90_days") shouldBe null
    row.getAs[java.lang.Long]("revision_seconds_to_identity_revert_within_90_days")           shouldBe null
  }

  it should "set revision_is_identity_revert_within_90_days true when the reverting delay is within 90d" in {
    // rev 102 (is_identity_revert=true) reverted rev 101 which was reverted in 3600s (<= 90d).
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT
           'enwiki' AS wiki_db, 'revision' AS event_entity, 'edit' AS event_type,
           '2024-01-15 10:00:00.0' AS event_timestamp,
           CAST(NULL AS BIGINT) AS event_user_id, CAST(NULL AS BIGINT) AS event_user_central_id,
           CAST(NULL AS STRING) AS event_user_text_historical,
           CAST(NULL AS BOOLEAN) AS event_user_is_anonymous, CAST(NULL AS BOOLEAN) AS event_user_is_temporary,
           CAST(NULL AS BOOLEAN) AS event_user_is_permanent, CAST(NULL AS STRING) AS event_user_registration_timestamp,
           CAST(NULL AS BOOLEAN) AS event_user_is_created_by_self,
           CAST(1 AS BIGINT) AS page_id, CAST(NULL AS STRING) AS page_title_historical,
           CAST(0 AS INT) AS page_namespace_historical,
           CAST(101 AS BIGINT) AS revision_id, CAST(NULL AS BIGINT) AS revision_parent_id,
           CAST(NULL AS BOOLEAN) AS revision_minor_edit, CAST(NULL AS BIGINT) AS revision_text_bytes,
           CAST(NULL AS BIGINT) AS revision_text_bytes_diff, CAST(NULL AS STRING) AS revision_text_sha1,
           CAST(NULL AS ARRAY<STRING>) AS revision_tags, CAST(NULL AS BOOLEAN) AS page_namespace_is_content_historical,
           CAST(NULL AS ARRAY<STRING>) AS event_user_is_bot_by_historical, CAST(NULL AS ARRAY<STRING>) AS revision_deleted_parts, CAST(NULL AS BIGINT) AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
           true AS revision_is_identity_reverted, CAST(102 AS BIGINT) AS revision_first_identity_reverting_revision_id,
           CAST(3600 AS BIGINT) AS revision_seconds_to_identity_revert, false AS revision_is_identity_revert,
           '2024-01' AS snapshot
         UNION ALL
         SELECT
           'enwiki', 'revision', 'edit', '2024-01-15 11:00:00.0',
           CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS STRING),
           CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), CAST(NULL AS BOOLEAN),
           CAST(1 AS BIGINT), CAST(NULL AS STRING), CAST(0 AS INT),
           CAST(102 AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT),
           CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS BOOLEAN),
           CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS BIGINT),
           CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS BIGINT), CAST(NULL AS STRING),
           CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN),
           CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN),
           false, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), true,
           '2024-01'"""
    )

    val rows = projected().collect().sortBy(_.getAs[Long]("revision_id"))
    rows(1).getAs[Long]("revision_id")                                      shouldEqual 102L
    rows(1).getAs[Boolean]("revision_is_identity_revert_within_90_days")    shouldEqual true
  }

  it should "exclude rows from other snapshots" in {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT 'enwiki' AS wiki_db, 'revision' AS event_entity, 'edit' AS event_type,
                '2024-01-15 10:00:00.0' AS event_timestamp,
                CAST(NULL AS BIGINT) AS event_user_id,
                CAST(NULL AS BIGINT) AS event_user_central_id,
                CAST(NULL AS STRING) AS event_user_text_historical,
                CAST(NULL AS BOOLEAN) AS event_user_is_anonymous,
                CAST(NULL AS BOOLEAN) AS event_user_is_temporary,
                CAST(NULL AS BOOLEAN) AS event_user_is_permanent,
                CAST(NULL AS STRING) AS event_user_registration_timestamp,
                CAST(NULL AS BOOLEAN) AS event_user_is_created_by_self,
                CAST(1 AS BIGINT) AS page_id, CAST(NULL AS STRING) AS page_title_historical,
                CAST(0 AS INT) AS page_namespace_historical,
                CAST(101 AS BIGINT) AS revision_id, CAST(NULL AS BIGINT) AS revision_parent_id,
                CAST(NULL AS BOOLEAN) AS revision_minor_edit,
                CAST(NULL AS BIGINT) AS revision_text_bytes, CAST(NULL AS BIGINT) AS revision_text_bytes_diff,
                CAST(NULL AS STRING) AS revision_text_sha1, CAST(NULL AS ARRAY<STRING>) AS revision_tags,
                CAST(NULL AS BOOLEAN) AS page_namespace_is_content_historical,
                CAST(NULL AS ARRAY<STRING>) AS event_user_is_bot_by_historical,
                CAST(NULL AS ARRAY<STRING>) AS revision_deleted_parts,
                CAST(NULL AS BIGINT) AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted,
                CAST(NULL AS BIGINT) AS revision_first_identity_reverting_revision_id,
                CAST(NULL AS BIGINT) AS revision_seconds_to_identity_revert,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_revert,
                '2023-12' AS snapshot"""
    )

    projected().count() shouldEqual 0
  }

  it should "exclude rows with event_timestamp at or beyond the next month" in {
    // wmf.mediawiki_history sqoop dumps overlap into the next month, so a Jan snapshot
    // can contain revisions with Feb timestamps. Without the upper bound those rows would
    // be inserted as source='snapshot' and duplicate source='events' rows for the same revision_id.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT 'enwiki' AS wiki_db, 'revision' AS event_entity, 'edit' AS event_type,
                '2024-02-01 00:00:00.0' AS event_timestamp,
                CAST(NULL AS BIGINT) AS event_user_id,
                CAST(NULL AS BIGINT) AS event_user_central_id,
                CAST(NULL AS STRING) AS event_user_text_historical,
                CAST(NULL AS BOOLEAN) AS event_user_is_anonymous,
                CAST(NULL AS BOOLEAN) AS event_user_is_temporary,
                CAST(NULL AS BOOLEAN) AS event_user_is_permanent,
                CAST(NULL AS STRING) AS event_user_registration_timestamp,
                CAST(NULL AS BOOLEAN) AS event_user_is_created_by_self,
                CAST(1 AS BIGINT) AS page_id, CAST(NULL AS STRING) AS page_title_historical,
                CAST(0 AS INT) AS page_namespace_historical,
                CAST(999 AS BIGINT) AS revision_id, CAST(NULL AS BIGINT) AS revision_parent_id,
                CAST(NULL AS BOOLEAN) AS revision_minor_edit,
                CAST(NULL AS BIGINT) AS revision_text_bytes, CAST(NULL AS BIGINT) AS revision_text_bytes_diff,
                CAST(NULL AS STRING) AS revision_text_sha1, CAST(NULL AS ARRAY<STRING>) AS revision_tags,
                CAST(NULL AS BOOLEAN) AS page_namespace_is_content_historical,
                CAST(NULL AS ARRAY<STRING>) AS event_user_is_bot_by_historical,
                CAST(NULL AS ARRAY<STRING>) AS revision_deleted_parts,
                CAST(NULL AS BIGINT) AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted,
                CAST(NULL AS BIGINT) AS revision_first_identity_reverting_revision_id,
                CAST(NULL AS BIGINT) AS revision_seconds_to_identity_revert,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_revert,
                '2024-01' AS snapshot"""
    )
    projected().count() shouldEqual 0
  }

  // ---- MERGE execution — event-row cleanup bounds ----
  //
  // The snapshot for month M lands ~3 days into month M+1. At merge time, M+1 events rows
  // are already in the table. The upper bound (< monthEnd) guards those in-flight rows
  // from being deleted.

  "MWHistorySnapshotMerger MERGE" should "delete source='events' rows within the snapshot month" in {
    spark.sql("DELETE FROM local.db.test_target WHERE true")
    insertEventsRow(revisionId = 201L, eventTimestamp = "2024-01-15 10:00:00")
    registerSource()
    spark.sql(MWHistorySnapshotMerger.buildMergeSQL(mergeParams))
    spark.sql("SELECT * FROM local.db.test_target WHERE source = 'events' AND revision_id = 201")
      .count() shouldEqual 0
  }

  it should "keep source='events' rows for the month after the snapshot month" in {
    spark.sql("DELETE FROM local.db.test_target WHERE true")
    // Simulates 3 days of Feb dailies already written when the Jan snapshot lands
    insertEventsRow(revisionId = 301L, eventTimestamp = "2024-02-03 10:00:00")
    registerSource()
    spark.sql(MWHistorySnapshotMerger.buildMergeSQL(mergeParams))
    spark.sql("SELECT * FROM local.db.test_target WHERE source = 'events' AND revision_id = 301")
      .count() shouldEqual 1
  }

  // ---- MERGE SQL — event-row cleanup upper bound ----
  //
  // The snapshot for month M lands ~3 days into month M+1. The upper bound (< monthEnd)
  // guards M+1 in-flight events from being deleted. No lower bound: stale events rows
  // from prior months are cleaned up for free.

  "MWHistorySnapshotMerger.buildMergeSQL" should "include the next-month upper bound to protect in-flight events" in {
    // Without this bound, Feb events rows already in the table when the Jan snapshot merger
    // runs would be deleted.
    val sql = MWHistorySnapshotMerger.buildMergeSQL(params)
    sql should include ("TIMESTAMP '2024-02-01 00:00:00'")
  }

  it should "not include a lower bound so stale events rows from prior months are also cleaned up" in {
    val sql = MWHistorySnapshotMerger.buildMergeSQL(params)
    sql should not include "TIMESTAMP '2024-01-01 00:00:00'"
  }

  it should "roll the upper bound into the next year for a December snapshot" in {
    val sql = MWHistorySnapshotMerger.buildMergeSQL(params.copy(snapshot = "2024-12"))
    sql should include ("TIMESTAMP '2025-01-01 00:00:00'")
  }

  // ---- revision_deleted_parts projection ----

  "MWHistorySnapshotMerger" should "project revision_deleted_parts from wmf.mediawiki_history" in {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_mwh AS
         SELECT
           'enwiki'                          AS wiki_db,
           'revision'                        AS event_entity,
           'edit'                            AS event_type,
           '2024-01-15 10:00:00.0'           AS event_timestamp,
           CAST(NULL AS BIGINT)              AS event_user_id,
           CAST(NULL AS BIGINT)              AS event_user_central_id,
           CAST(NULL AS STRING)              AS event_user_text_historical,
           CAST(NULL AS BOOLEAN)             AS event_user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS event_user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS event_user_is_permanent,
           CAST(NULL AS STRING)              AS event_user_registration_timestamp,
           CAST(NULL AS BOOLEAN)             AS event_user_is_created_by_self,
           CAST(1 AS BIGINT)                 AS page_id,
           CAST(NULL AS STRING)              AS page_title_historical,
           CAST(0 AS INT)                    AS page_namespace_historical,
           CAST(101 AS BIGINT)               AS revision_id,
           CAST(NULL AS BIGINT)              AS revision_parent_id,
           CAST(NULL AS BOOLEAN)             AS revision_minor_edit,
           CAST(NULL AS BIGINT)              AS revision_text_bytes,
           CAST(NULL AS BIGINT)              AS revision_text_bytes_diff,
           CAST(NULL AS STRING)              AS revision_text_sha1,
           CAST(NULL AS ARRAY<STRING>)       AS revision_tags,
           CAST(NULL AS BOOLEAN)             AS page_namespace_is_content_historical,
           CAST(NULL AS ARRAY<STRING>)       AS event_user_is_bot_by_historical,
           array('text', 'comment')           AS revision_deleted_parts,
           CAST(NULL AS BIGINT)              AS event_user_revision_count,
           CAST(NULL AS ARRAY<STRING>) AS event_user_groups_historical,
           CAST(NULL AS BIGINT)              AS user_id,
           CAST(NULL AS STRING)              AS user_text_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_anonymous,
           CAST(NULL AS BOOLEAN)             AS user_is_temporary,
           CAST(NULL AS BOOLEAN)             AS user_is_permanent,
           CAST(NULL AS ARRAY<STRING>)       AS user_groups_historical,
           CAST(NULL AS ARRAY<STRING>)       AS user_is_bot_by_historical,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_self,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_system,
           CAST(NULL AS BOOLEAN)             AS user_is_created_by_peer,
           false                             AS revision_is_identity_reverted,
           CAST(NULL AS BIGINT)              AS revision_first_identity_reverting_revision_id,
           CAST(NULL AS BIGINT)              AS revision_seconds_to_identity_revert,
           false                             AS revision_is_identity_revert,
           '2024-01'                         AS snapshot"""
    )

    val row = projected().collect()(0)
    row.getAs[Seq[String]]("revision_deleted_parts") shouldEqual Seq("text", "comment")
  }
}
