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
    // Minimal table for MERGE 3 (tags) integration test.
    spark.sql(
      """CREATE TABLE IF NOT EXISTS local.db.cm_format_target (
           wiki_id         STRING,
           revision_id     BIGINT,
           event_timestamp TIMESTAMP,
           revision_tags   ARRAY<STRING>,
           control_map     MAP<STRING,STRING>,
           row_update_dt   TIMESTAMP
         ) USING iceberg"""
    )
    spark.sql(
      """INSERT INTO local.db.cm_format_target VALUES (
           'enwiki', 101L, TIMESTAMP '2024-01-15 10:00:00',
           CAST(NULL AS ARRAY<STRING>),
           CAST(NULL AS MAP<STRING,STRING>),
           CAST(NULL AS TIMESTAMP)
         )"""
    )
    // Full-schema table for MERGE 1 (revision INSERT) integration test.
    spark.sql(
      """CREATE TABLE IF NOT EXISTS local.db.cm_format_m1_target (
           source                                        STRING,
           wiki_id                                       STRING,
           event_meta_id                                 STRING,
           event_entity                                  STRING,
           event_type                                    STRING,
           event_timestamp                               TIMESTAMP,
           event_user_id                                 BIGINT,
           event_user_central_id                         BIGINT,
           event_user_text_historical                    STRING,
           event_user_is_anonymous                       BOOLEAN,
           event_user_is_temporary                       BOOLEAN,
           event_user_is_permanent                       BOOLEAN,
           event_user_registration_timestamp             TIMESTAMP,
           page_id                                       BIGINT,
           page_title_historical                         STRING,
           page_namespace_historical                     INT,
           revision_id                                   BIGINT,
           revision_parent_id                            BIGINT,
           revision_minor_edit                           BOOLEAN,
           revision_text_bytes                           BIGINT,
           revision_text_bytes_diff                      BIGINT,
           revision_text_sha1                            STRING,
           revision_tags                                 ARRAY<STRING>,
           revision_deleted_parts                        ARRAY<STRING>,
           page_namespace_is_content_historical          BOOLEAN,
           event_user_is_bot_by_historical               ARRAY<STRING>,
           event_user_revision_count                     BIGINT,
           event_user_groups_historical                  ARRAY<STRING>,
           user_id                                       BIGINT,
           user_text_historical                          STRING,
           user_is_anonymous                             BOOLEAN,
           user_is_temporary                             BOOLEAN,
           user_is_permanent                             BOOLEAN,
           user_groups_historical                        ARRAY<STRING>,
           user_is_bot_by_historical                     ARRAY<STRING>,
           user_is_created_by_self                       BOOLEAN,
           user_is_created_by_system                     BOOLEAN,
           user_is_created_by_peer                       BOOLEAN,
           revision_is_identity_reverted                 BOOLEAN,
           revision_first_identity_reverting_revision_id BIGINT,
           revision_seconds_to_identity_revert           BIGINT,
           revision_is_identity_revert                   BOOLEAN,
           event_user_is_cross_wiki                      BOOLEAN,
           page_is_deleted                               BOOLEAN,
           revision_is_deleted_by_page_deletion          BOOLEAN,
           user_central_id                               BIGINT,
           control_map                                   MAP<STRING,STRING>,
           row_update_dt                                 TIMESTAMP
         ) USING iceberg"""
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
    spark.sql(MWHistoryDeltaRevisionSQL.buildRevisionTagsMergeSQL(p))
    val dt = spark.sql(
      "SELECT control_map['tags_update_dt'] FROM local.db.cm_format_target WHERE wiki_id = 'enwiki' AND revision_id = 101"
    ).collect()(0).getString(0)
    dt should fullyMatch regex iso8601
    dt shouldEqual "2024-01-15T10:00:01.370Z"
  }

  // --- Integration test: MERGE 1 INSERT path ---

  "revision_update_dt written by buildMergeSQL WHEN NOT MATCHED INSERT" should "match ISO-8601 UTC in the Iceberg table" in {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_source_m1 AS
         SELECT
           t.wiki_id,
           t.page_change_kind,
           named_struct(
             'rev_id',              t.rev_id,
             'rev_parent_id',       t.rev_parent_id,
             'rev_dt',              t.rev_dt,
             'rev_size',            t.rev_size,
             'rev_sha1',            t.rev_sha1,
             'is_minor_edit',       false,
             'is_content_visible',  true,
             'is_editor_visible',   true,
             'is_comment_visible',  true,
             'tags',                array(),
             'editor', named_struct(
               'user_id',          t.user_id,
               'user_central_id',  t.user_central_id,
               'user_text',        t.user_text,
               'is_temp',          false,
               'groups',           array(),
               'registration_dt',  CAST(NULL AS STRING),
               'edit_count',       t.edit_count
             )
           ) AS revision,
           named_struct(
             'revision', named_struct(
               'rev_size', t.prior_rev_size,
               'rev_sha1', CAST(NULL AS STRING)
             )
           ) AS prior_state,
           named_struct(
             'page_id',      t.page_id,
             'page_title',   t.page_title,
             'namespace_id', 0
           ) AS page,
           named_struct('dt', t.meta_dt) AS meta,
           2024 AS year, 1 AS month, 15 AS day
         FROM (SELECT * FROM VALUES
           ('enwiki', 'edit', 501L, 500L, '2024-01-15T09:00:00Z', 100, 'abc123', 90, 42L, 42L, 'TestUser', 10L, 202L, 'TestPage', '2024-01-15T10:00:01.370Z')
         ) AS t(wiki_id, page_change_kind, rev_id, rev_parent_id, rev_dt, rev_size, rev_sha1,
                prior_rev_size, user_id, user_central_id, user_text, edit_count, page_id, page_title, meta_dt)"""
    )
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_namespaces_m1 AS
         SELECT 'enwiki' AS dbname, 0 AS namespace, 1 AS namespace_is_content, '2024-12' AS snapshot"""
    )
    val p = MWHistoryDeltaWriter.Params(
      pageChangeTable    = "test_source_m1",
      targetTable        = "db.cm_format_m1_target",
      namespacesTable    = "test_namespaces_m1",
      namespacesSnapshot = "2024-12",
      catalog            = "local",
      year = 2024, month = 1, day = 15
    )
    spark.sql(MWHistoryDeltaRevisionSQL.buildRevisionEventMergeSQL(p))
    val dt = spark.sql(
      "SELECT control_map['revision_update_dt'] FROM local.db.cm_format_m1_target WHERE wiki_id = 'enwiki' AND revision_id = 501"
    ).collect()(0).getString(0)
    dt should fullyMatch regex iso8601
    dt shouldEqual "2024-01-15T10:00:01.370Z"
  }
}
