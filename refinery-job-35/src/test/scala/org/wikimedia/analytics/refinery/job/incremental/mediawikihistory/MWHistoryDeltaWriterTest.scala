package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Tests for MWHistoryDeltaWriter.
 *
 * All tests run the CTE chain (buildIncomingSQL + SELECT * FROM incoming, or
 * buildBackPatchCteSQL + SELECT * FROM back_patch) against synthetic temp views,
 * without Iceberg. The MERGE itself is not tested here — the SQL is structurally
 * identical to what Iceberg executes.
 *
 * The source view mirrors the mediawiki.page_change.v1 nested struct layout.
 * Field paths match the verified schema at
 * ~/wmf/gitlab/schemas-event-primary/jsonschema/mediawiki/page/change/latest.yaml
 */
class MWHistoryDeltaWriterTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MWHistoryDeltaWriterTest")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  val params: MWHistoryDeltaWriter.Params = MWHistoryDeltaWriter.Params(
    sourceTable        = "test_source",
    targetTable        = "test_target",
    namespacesTable    = "test_namespaces",
    namespacesSnapshot = "2024-12",
    tagsTable          = "test_tags",
    visibilityTable    = "test_visibility",
    userChangeTable    = "test_user_source",
    year = 2024, month = 1, day = 15
  )

  def incoming()         = spark.sql(MWHistoryDeltaWriter.buildIncomingSQL(params)      + "\nSELECT * FROM incoming")
  def backPatch()        = spark.sql(MWHistoryDeltaWriter.buildBackPatchCteSQL(params)  + "\nSELECT * FROM back_patch")
  def latestTags()       = spark.sql(MWHistoryDeltaWriter.buildTagsCteSQL(params)       + "\nSELECT * FROM latest_tags")
  def latestVisibility() = spark.sql(MWHistoryDeltaWriter.buildVisibilityCteSQL(params) + "\nSELECT * FROM latest_visibility")
  def pageIncoming()     = spark.sql(MWHistoryDeltaWriter.buildPageIncomingSQL(params)  + "\nSELECT * FROM page_incoming")
  def userIncoming()     = spark.sql(MWHistoryDeltaWriter.buildUserIncomingSQL(params)  + "\nSELECT * FROM user_incoming")

  /** Empty target — only needed for the revert_seed CTE, not for byte diff. */
  def registerEmptyTarget(): Unit =
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT
           CAST(NULL AS STRING)    AS wiki_id,
           CAST(NULL AS BIGINT)    AS revision_id,
           CAST(NULL AS STRING)    AS revision_text_sha1,
           CAST(NULL AS BIGINT)    AS page_id,
           CAST(NULL AS TIMESTAMP) AS event_timestamp
         WHERE 1 = 0"""
    )

  def registerNamespaces(): Unit =
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_namespaces AS
         SELECT 'enwiki' AS dbname, 0 AS namespace, 1 AS namespace_is_content, '2024-12' AS snapshot
         UNION ALL
         SELECT 'enwiki', 1, 0, '2024-12'"""
    )

  /**
   * Registers synthetic page_change source rows matching the v1 nested struct layout.
   *
   * Columns (positional VALUES):
   *   wiki_id, page_change_kind, rev_id, rev_parent_id, rev_dt, rev_size, rev_sha1,
   *   prior_rev_size (NULL for page creates), user_id (NULL for anon), user_central_id,
   *   user_text, is_temp, groups (array), registration_dt (string or NULL),
   *   edit_count, namespace_id, page_id, page_title, meta_dt
   */
  def registerSourceWith(rows: String): Unit =
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_source AS
          SELECT
            t.wiki_id,
            t.page_change_kind,
            named_struct(
              'rev_id',        t.rev_id,
              'rev_parent_id', t.rev_parent_id,
              'rev_dt',        t.rev_dt,
              'rev_size',      t.rev_size,
              'rev_sha1',      t.rev_sha1,
              'is_minor_edit',       false,
              'is_content_visible',  true,
              'is_editor_visible',   true,
              'is_comment_visible',  true,
              'tags',                array(),
              'editor', named_struct(
                'user_id',          t.user_id,
                'user_central_id',  t.user_central_id,
                'user_text',        t.user_text,
                'is_temp',          t.is_temp,
                'groups',           t.groups,
                'registration_dt',  t.registration_dt,
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
              'page_id',     t.page_id,
              'page_title',  t.page_title,
              'namespace_id', t.namespace_id
            ) AS page,
            named_struct('dt', t.meta_dt) AS meta,
            2024 AS year, 1 AS month, 15 AS day
          FROM (
            SELECT * FROM VALUES $rows
          ) AS t(wiki_id, page_change_kind, rev_id, rev_parent_id, rev_dt, rev_size, rev_sha1,
                 prior_rev_size, user_id, user_central_id, user_text, is_temp, groups, registration_dt,
                 edit_count, namespace_id, page_id, page_title, meta_dt)"""
    )

  /**
   * Registers synthetic revision_tags_change source rows.
   * Columns: wiki_id (mapped to `database`), rev_id, tags, meta_dt
   */
  def registerTagsWith(rows: String): Unit =
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_tags AS
          SELECT
            t.wiki_id  AS database,
            t.rev_id   AS rev_id,
            t.tags     AS tags,
            named_struct('dt', t.meta_dt) AS meta,
            2024 AS year, 1 AS month, 15 AS day
          FROM (SELECT * FROM VALUES $rows) AS t(wiki_id, rev_id, tags, meta_dt)"""
    )

  /**
   * Registers synthetic revision_visibility_change source rows.
   * Columns: wiki_id (mapped to `database`), rev_id, text_visible, user_visible, comment_visible, meta_dt
   */
  def registerVisibilityWith(rows: String): Unit =
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_visibility AS
          SELECT
            t.wiki_id AS database,
            t.rev_id  AS rev_id,
            named_struct(
              'text',    t.text_visible,
              'user',    t.user_visible,
              'comment', t.comment_visible
            ) AS visibility,
            named_struct('dt', t.meta_dt) AS meta,
            2024 AS year, 1 AS month, 15 AS day
          FROM (SELECT * FROM VALUES $rows)
            AS t(wiki_id, rev_id, text_visible, user_visible, comment_visible, meta_dt)"""
    )

  /**
   * Registers synthetic page_change source rows for page events (move, delete, undelete).
   * The meta struct includes both dt (for ordering) and id (for event_meta_id / MERGE 5 key).
   * The prior_state.page struct carries the old title/namespace for moves; pass NULL for
   * prior_page_title and prior_namespace_id for delete/undelete events.
   *
   * Columns (positional VALUES):
   *   wiki_id, page_change_kind, dt (top-level event time), meta_id, meta_dt,
   *   prior_page_title (NULL for non-moves), prior_namespace_id (NULL for non-moves),
   *   user_id (NULL for anon), user_central_id, user_text, is_temp, groups,
   *   registration_dt (string or NULL), page_id, page_title, namespace_id
   */
  def registerPageEventWith(rows: String): Unit =
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_source AS
          SELECT
            t.wiki_id,
            t.page_change_kind,
            t.dt,
            named_struct(
              'id', t.meta_id,
              'dt', t.meta_dt
            ) AS meta,
            named_struct(
              'page', named_struct(
                'page_title',   t.prior_page_title,
                'namespace_id', t.prior_namespace_id
              )
            ) AS prior_state,
            named_struct(
              'user_id',         t.user_id,
              'user_central_id', t.user_central_id,
              'user_text',       t.user_text,
              'is_temp',         t.is_temp,
              'groups',          t.groups,
              'registration_dt', t.registration_dt
            ) AS performer,
            named_struct(
              'page_id',      t.page_id,
              'page_title',   t.page_title,
              'namespace_id', t.namespace_id
            ) AS page,
            2024 AS year, 1 AS month, 15 AS day
          FROM (
            SELECT * FROM VALUES $rows
          ) AS t(wiki_id, page_change_kind, dt, meta_id, meta_dt,
                 prior_page_title, prior_namespace_id,
                 user_id, user_central_id, user_text, is_temp, groups, registration_dt,
                 page_id, page_title, namespace_id)"""
    )

  def registerUserEventWith(rows: String): Unit =
    spark.sql(
      s"""CREATE OR REPLACE TEMP VIEW test_user_source AS
          SELECT
            t.wiki_id,
            t.user_change_kind,
            CAST(t.dt AS STRING)      AS dt,
            named_struct(
              'id', t.meta_id,
              'dt', t.meta_dt
            ) AS meta,
            t.is_autocreate,
            named_struct(
              'user_id',         t.user_id,
              'user_central_id', t.user_central_id,
              'user_text',       t.user_text,
              'is_temp',         t.is_temp,
              'edit_count',      t.edit_count,
              'groups',          t.groups,
              'registration_dt', t.registration_dt
            ) AS user,
            named_struct(
              'user', named_struct(
                'user_id',         t.prior_user_id,
                'user_central_id', t.user_central_id,
                'user_text',       t.prior_user_text,
                'is_temp',         t.prior_is_temp,
                'edit_count',      t.edit_count,
                'groups',          t.prior_groups,
                'registration_dt', t.registration_dt
              )
            ) AS prior_state,
            named_struct(
              'user_id',         t.performer_user_id,
              'user_central_id', t.performer_user_central_id,
              'user_text',       t.performer_user_text,
              'is_temp',         t.performer_is_temp,
              'registration_dt', t.performer_registration_dt,
              'groups',          t.performer_groups
            ) AS performer,
            ${params.year} AS year, ${params.month} AS month, ${params.day} AS day
          FROM (
            SELECT * FROM VALUES $rows
          ) AS t(wiki_id, user_change_kind, dt, meta_id, meta_dt,
                 is_autocreate,
                 user_id, user_central_id, user_text, is_temp, edit_count,
                 groups, registration_dt,
                 prior_user_id, prior_user_text, prior_groups, prior_is_temp,
                 performer_user_id, performer_user_central_id, performer_user_text,
                 performer_is_temp, performer_registration_dt, performer_groups)"""
    )

  // ---- Argument parsing ----

  "MWHistoryDeltaWriter.parseArgs" should "map CLI flags to Params fields" in {
    val p = MWHistoryDeltaWriter.parseArgs(Array(
      "--source_table",       "event.mediawiki_page_change",
      "--target_table",       "analytics.mediawiki_history_incremental_v1",
      "--namespaces_table",   "wmf_raw.mediawiki_project_namespace_map",
      "--namespaces_snapshot", "2024-12",
      "--tags_table",         "event.mediawiki_revision_tags_change",
      "--visibility_table",   "event.mediawiki_revision_visibility_change",
      "--user_change_table",  "event.mediawiki_user_change",
      "--year",  "2024",
      "--month", "1",
      "--day",   "15"
    ))
    p.sourceTable        shouldEqual "event.mediawiki_page_change"
    p.targetTable        shouldEqual "analytics.mediawiki_history_incremental_v1"
    p.namespacesTable    shouldEqual "wmf_raw.mediawiki_project_namespace_map"
    p.namespacesSnapshot shouldEqual "2024-12"
    p.tagsTable          shouldEqual "event.mediawiki_revision_tags_change"
    p.visibilityTable    shouldEqual "event.mediawiki_revision_visibility_change"
    p.userChangeTable    shouldEqual "event.mediawiki_user_change"
    p.year               shouldEqual 2024
    p.month              shouldEqual 1
    p.day                shouldEqual 15
  }

  // ---- Field mapping ----

  "MWHistoryDeltaWriter" should "map basic fields from page_change to the target schema" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 500, 'sha-A',
          400L, 42L, 999L, 'Alice', false, array('editor'), '2000-01-01T00:00:00Z', 7L,
          0, 1L, 'Main_Page', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[String]("source")                             shouldEqual "events"
    row.getAs[String]("wiki_id")                            shouldEqual "enwiki"
    row.getAs[String]("event_entity")                       shouldEqual "revision"
    row.getAs[String]("event_type")                         shouldEqual "edit"
    row.getAs[Long]("revision_id")                          shouldEqual 101L
    row.getAs[Long]("revision_parent_id")                   shouldEqual 100L
    row.getAs[Long]("revision_text_bytes")                  shouldEqual 500L
    row.getAs[Long]("revision_text_bytes_diff")             shouldEqual 100L  // 500 - 400
    row.getAs[String]("revision_text_sha1")                 shouldEqual "sha-A"
    row.getAs[String]("event_user_text_historical")         shouldEqual "Alice"
    row.getAs[Long]("event_user_id")                        shouldEqual 42L
    row.getAs[Long]("event_user_central_id")                shouldEqual 999L
    row.getAs[Boolean]("event_user_is_anonymous")           shouldEqual false
    row.getAs[Boolean]("event_user_is_temporary")           shouldEqual false
    row.getAs[Boolean]("event_user_is_permanent")           shouldEqual true
    row.getAs[Int]("page_namespace_historical")             shouldEqual 0
    row.getAs[Boolean]("page_namespace_is_content_historical") shouldEqual true
    row.getAs[Long]("event_user_revision_count")             shouldEqual 7L
  }

  // ---- is_anonymous derived from user_id ----

  it should "derive is_anonymous as true when user_id is null" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), '192.168.1.1', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[Boolean]("event_user_is_anonymous") shouldEqual true
    row.getAs[Boolean]("event_user_is_permanent") shouldEqual false
  }

  // ---- registration_dt ----

  it should "populate event_user_registration_timestamp from revision.editor.registration_dt" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 42L, CAST(NULL AS BIGINT), 'Alice', false, array(), '2000-06-15T12:00:00Z', CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val ts = incoming().collect()(0).getAs[java.sql.Timestamp]("event_user_registration_timestamp")
    ts should not be null
    ts.toString should startWith("2000-06-15")
  }

  it should "leave event_user_registration_timestamp null when registration_dt is absent" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 42L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[java.sql.Timestamp]("event_user_registration_timestamp") shouldBe null
  }

  // ---- Byte diff from prior_state ----

  it should "compute byte diff from prior_state.revision.rev_size" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 700, 'sha',
          500L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[Long]("revision_text_bytes_diff") shouldEqual 200L  // 700 - 500
  }

  it should "set byte diff to null for page creates where prior_state has no rev_size" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'create', 101L, 0L, '2024-01-15T10:00:00Z', 300, 'sha',
          CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[java.lang.Long]("revision_text_bytes_diff") shouldBe null
  }

  // ---- Page-import / old rev_dt filter ----

  it should "exclude events where rev_dt is more than 90 days before the run date (page import pattern)" in {
    registerEmptyTarget()
    registerNamespaces()
    // rev 101: rev_dt from 2006 — a page-import event ingested on the run date; must be excluded.
    // rev 102: rev_dt from the run date — a normal edit; must be included.
    registerSourceWith(
      """('enwiki', 'create', 101L, 0L,   '2006-03-15T10:00:00Z', 300, 'sha-old', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit',   102L, 101L, '2024-01-15T10:00:00Z', 400, 'sha-new', 300L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:02Z')"""
    )

    val rows = incoming().collect()
    rows.length                             shouldEqual 1
    rows(0).getAs[Long]("revision_id")      shouldEqual 102L
  }

  it should "include events where rev_dt is within 90 days of the run date" in {
    registerEmptyTarget()
    registerNamespaces()
    // rev 101: rev_dt = 89 days before run date (2024-01-15 - 89d = 2023-10-18) — within the 90-day window.
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2023-10-18T10:00:00Z', 500, 'sha', 400L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect().length shouldEqual 1
  }

  // ---- Deduplication ----

  it should "keep only the latest event when the same revision arrives twice" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 500, 'sha', 400L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:02Z'),
         ('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 600, 'sha', 400L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val rows = incoming().collect()
    rows.length                              shouldEqual 1
    rows(0).getAs[Long]("revision_text_bytes") shouldEqual 500L  // later meta_dt row wins
  }

  // ---- Bot classification ----

  it should "classify a bot-by-name user" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'ClueBot', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") shouldEqual Seq("name")
  }

  it should "classify a bot-by-group user" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'SomeUser', false, array('bot', 'sysop'), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") shouldEqual Seq("group")
  }

  it should "classify a user that is both bot-by-name and bot-by-group" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'ClueBot', false, array('bot'), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") shouldEqual Seq("name", "group")
  }

  it should "not classify a normal user as a bot" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array('editor'), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    incoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") shouldBe empty
  }

  // ---- Revert detection (in-batch) ----

  it should "detect a revert when the same sha1 repeats within 90d in the same batch" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L,   '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit', 103L, 102L, '2024-01-15T11:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T11:00:01Z')"""
    )

    val rows = incoming().collect().sortBy(_.getAs[Long]("revision_id"))

    // Reverted row (rev 101): both tiers true, seconds set
    rows(0).getAs[Boolean]("revision_is_identity_reverted")                              shouldEqual true
    rows(0).getAs[Long]("revision_first_identity_reverting_revision_id")                 shouldEqual 103L
    rows(0).getAs[Long]("revision_seconds_to_identity_revert")                           shouldEqual 3600L
    rows(0).getAs[Boolean]("revision_is_identity_reverted_within_90_days")               shouldEqual true
    rows(0).getAs[Long]("revision_first_identity_reverting_revision_id_within_90_days")  shouldEqual 103L
    rows(0).getAs[Long]("revision_seconds_to_identity_revert_within_90_days")            shouldEqual 3600L
    // Not itself a revert: bounded false, authoritative NULL
    rows(0).getAs[Boolean]("revision_is_identity_revert_within_90_days")                 shouldEqual false
    rows(0).getAs[java.lang.Boolean]("revision_is_identity_revert")                      shouldBe null

    // Reverting row (rev 103): both tiers true
    rows(1).getAs[Boolean]("revision_is_identity_revert_within_90_days")                 shouldEqual true
    rows(1).getAs[Boolean]("revision_is_identity_revert")                                shouldEqual true
    // Not itself reverted: bounded false, authoritative NULL
    rows(1).getAs[Boolean]("revision_is_identity_reverted_within_90_days")               shouldEqual false
    rows(1).getAs[java.lang.Boolean]("revision_is_identity_reverted")                    shouldBe null
  }

  it should "not flag a revert when the sha1 repeats outside the 90d window" in {
    registerNamespaces()
    // Seed: rev 99 in the target with sha-X from 2023-10-01, more than 90 days before 2024-01-15.
    // The revert_seed CTE bounds to 90 days, so rev 99 is excluded from the seed window;
    // the incoming rev 103 cannot be detected as a revert.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, 1L AS page_id,
                to_timestamp('2023-10-01T10:00:00Z') AS event_timestamp"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 103L, 102L, '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:02Z')"""
    )

    val rows = incoming().collect()
    rows.length shouldEqual 1
    // Bounded tier: false (seed outside 90d window, not seen as a revert)
    rows(0).getAs[Boolean]("revision_is_identity_revert_within_90_days")   shouldEqual false
    // Authoritative tier: NULL (unknown, not detected within window)
    rows(0).getAs[java.lang.Boolean]("revision_is_identity_revert")         shouldBe null
  }

  it should "detect a cross-day revert using the seed from the target table" in {
    registerNamespaces()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, 1L AS page_id,
                to_timestamp('2024-01-14T16:00:00Z') AS event_timestamp"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T02:00:00Z', 300, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T02:00:01Z')"""
    )

    val row = incoming().collect()(0)
    // The incoming revision repeats the sha1 of a seed row — it IS a revert
    row.getAs[Boolean]("revision_is_identity_revert_within_90_days") shouldEqual true
    row.getAs[Boolean]("revision_is_identity_revert")                shouldEqual true
    // It has not itself been reverted in this batch
    row.getAs[Boolean]("revision_is_identity_reverted_within_90_days") shouldEqual false
    row.getAs[java.lang.Boolean]("revision_is_identity_reverted")      shouldBe null
  }

  it should "set bounded revert fields to false (not null) when sha1 is present but no revert detected" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 500, 'sha-unique',
          400L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted_within_90_days") shouldEqual false
    row.getAs[Boolean]("revision_is_identity_revert_within_90_days")   shouldEqual false
    row.getAs[java.lang.Boolean]("revision_is_identity_reverted")       shouldBe null
    row.getAs[java.lang.Boolean]("revision_is_identity_revert")         shouldBe null
  }

  // ---- Back-patch CTE ----

  it should "produce a back_patch row when an incoming revision is the first revert of a seed row" in {
    registerNamespaces()
    // Seed: rev 99 in target, sha-X, timestamped 10h before the incoming revision (both UTC).
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, 1L AS page_id,
                to_timestamp('2024-01-14T16:00:00Z') AS event_timestamp"""
    )
    // Incoming: rev 101 at 2024-01-15T02:00:00Z (10h after seed).
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T02:00:00Z', 300, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T02:00:01Z')"""
    )

    val rows = backPatch().collect()
    rows.length shouldEqual 1
    rows(0).getAs[Long]("target_revision_id")     shouldEqual 99L
    rows(0).getAs[Long]("first_reverting_rev_id") shouldEqual 101L
    rows(0).getAs[Long]("seconds_to_revert")      shouldEqual 10 * 3600L  // 10h
  }

  it should "produce no back_patch row when both reverted and reverting revisions are incoming" in {
    registerEmptyTarget()
    registerNamespaces()
    // Both rev 101 and rev 103 are incoming — back_patch only covers seed rows
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L,   '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit', 103L, 102L, '2024-01-15T11:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T11:00:01Z')"""
    )

    backPatch().count() shouldEqual 0
  }

  // ---- Tags CTE ----

  it should "produce one latest_tags row with correct fields" in {
    registerTagsWith(
      """('enwiki', 101L, array('mobile edit', 'wikieditor'), '2024-01-15T10:00:00Z')"""
    )

    val rows = latestTags().collect()
    rows.length shouldEqual 1
    rows(0).getAs[String]("wiki_id")                shouldEqual "enwiki"
    rows(0).getAs[Long]("revision_id")              shouldEqual 101L
    rows(0).getAs[Seq[String]]("revision_tags")     shouldEqual Seq("mobile edit", "wikieditor")
  }

  it should "deduplicate tags events by taking the latest meta.dt for the same revision" in {
    registerTagsWith(
      """('enwiki', 101L, array('first-tag'),  '2024-01-15T10:00:00Z'),
         ('enwiki', 101L, array('second-tag'), '2024-01-15T11:00:00Z')"""
    )

    val rows = latestTags().collect()
    rows.length shouldEqual 1
    rows(0).getAs[Seq[String]]("revision_tags") shouldEqual Seq("second-tag")
  }

  it should "produce one row per (wiki_id, rev_id) pair" in {
    registerTagsWith(
      """('enwiki',    101L, array('tag-a'), '2024-01-15T10:00:00Z'),
         ('dewiki',    202L, array('tag-b'), '2024-01-15T10:00:00Z'),
         ('enwiki',    303L, array('tag-c'), '2024-01-15T10:00:00Z')"""
    )

    latestTags().count() shouldEqual 3
  }

  it should "produce no back_patch row when the seed sha1 was already reverted before today" in {
    registerNamespaces()
    // Seed: rev 99 (sha-X, oldest) and rev 100 (sha-X, second — already reverted rev 99 yesterday)
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, 1L AS page_id,
                to_timestamp('2024-01-10T00:00:00Z') AS event_timestamp
         UNION ALL
         SELECT 'enwiki', 100L, 'sha-X', 1L,
                to_timestamp('2024-01-14T00:00:00Z')"""
    )
    // Incoming: rev 103 repeats sha-X again — it's a revert but NOT the first (sha1_rank=3);
    // sha1_rank=2 is already a seed row so the back_patch WHERE condition won't match.
    registerSourceWith(
      """('enwiki', 'edit', 103L, 102L, '2024-01-15T10:00:00Z', 300, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    backPatch().count() shouldEqual 0
  }

  // ---- Visibility CTE ----

  it should "produce one latest_visibility row with content in deleted_parts when text is hidden" in {
    registerVisibilityWith(
      """('enwiki', 101L, false, true, true, '2024-01-15T10:00:00Z')"""
    )

    val rows = latestVisibility().collect()
    rows.length shouldEqual 1
    rows(0).getAs[String]("wiki_id")                     shouldEqual "enwiki"
    rows(0).getAs[Long]("revision_id")                   shouldEqual 101L
    rows(0).getAs[Seq[String]]("revision_deleted_parts") shouldEqual Seq("text")
  }

  it should "set revision_deleted_parts to null when all visibility fields are true (restore)" in {
    registerVisibilityWith(
      """('enwiki', 101L, true, true, true, '2024-01-15T10:00:00Z')"""
    )

    latestVisibility().collect()(0).getAs[Seq[String]]("revision_deleted_parts") shouldBe null
  }

  it should "include all three parts when all visibility fields are false" in {
    registerVisibilityWith(
      """('enwiki', 101L, false, false, false, '2024-01-15T10:00:00Z')"""
    )

    val parts = latestVisibility().collect()(0).getAs[Seq[String]]("revision_deleted_parts")
    parts should contain allOf ("text", "user", "comment")
  }

  it should "deduplicate visibility events by taking the latest meta.dt for the same revision" in {
    registerVisibilityWith(
      """('enwiki', 101L, false, true, true, '2024-01-15T10:00:00Z'),
         ('enwiki', 101L, true,  true, true, '2024-01-15T11:00:00Z')"""
    )

    val rows = latestVisibility().collect()
    rows.length shouldEqual 1
    rows(0).getAs[Seq[String]]("revision_deleted_parts") shouldBe null
  }

  // ---- Page events (MERGE 5) ----

  "MWHistoryDeltaWriter page events" should "map basic fields from a move event" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-A', '2024-01-15T10:00:01Z',
          'Old_Title', 0,
          42L, 999L, 'Alice', false, array('sysop'), '2000-01-01T00:00:00Z',
          1L, 'New_Title', 0)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[String]("source")                              shouldEqual "events"
    row.getAs[String]("wiki_id")                             shouldEqual "enwiki"
    row.getAs[String]("event_meta_id")                       shouldEqual "uuid-A"
    row.getAs[String]("event_entity")                        shouldEqual "page"
    row.getAs[String]("event_type")                          shouldEqual "move"
    row.getAs[String]("page_title_historical")               shouldEqual "New_Title"
    row.getAs[Int]("page_namespace_historical")              shouldEqual 0
    row.getAs[Long]("page_id")                               shouldEqual 1L
    row.getAs[String]("event_user_text_historical")          shouldEqual "Alice"
    row.getAs[Long]("event_user_id")                         shouldEqual 42L
    row.getAs[Long]("event_user_central_id")                 shouldEqual 999L
    row.getAs[Boolean]("event_user_is_anonymous")            shouldEqual false
    row.getAs[Boolean]("event_user_is_temporary")            shouldEqual false
    row.getAs[Boolean]("event_user_is_permanent")            shouldEqual true
    row.getAs[java.lang.Long]("revision_id")                 shouldBe null
    row.getAs[Boolean]("page_namespace_is_content_historical") shouldEqual true
  }

  it should "use page.page_title (post-move destination) as historical title for move events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-B', '2024-01-15T10:00:01Z',
          'Old_Title', 0,
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'New_Title', 4)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[String]("page_title_historical")  shouldEqual "New_Title"
    row.getAs[Int]("page_namespace_historical") shouldEqual 4
  }

  it should "strip namespace prefix from page_title_historical for non-main-namespace pages" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-E', '2024-01-15T10:00:01Z',
          'User:Old_User/sandbox', 2,
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'User:New_User/sandbox', 2)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[String]("page_title_historical")  shouldEqual "New_User/sandbox"
    row.getAs[Int]("page_namespace_historical") shouldEqual 2
  }

  it should "fall back to page.page_title as historical title for delete events (no prior_state.page)" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-C', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'Target_Page', 0)"""
    )

    pageIncoming().collect()(0).getAs[String]("page_title_historical") shouldEqual "Target_Page"
  }

  it should "map undelete to 'restore' for event_type" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'undelete', '2024-01-15T10:00:00Z', 'uuid-D', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'Some_Page', 0)"""
    )

    pageIncoming().collect()(0).getAs[String]("event_type") shouldEqual "restore"
  }

  it should "set all revision and revert fields to null for page events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-E', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'A', 0)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[java.lang.Long]("revision_id")                                           shouldBe null
    row.getAs[java.lang.Boolean]("revision_is_identity_reverted")                      shouldBe null
    row.getAs[java.lang.Boolean]("revision_is_identity_revert")                        shouldBe null
    row.getAs[java.lang.Boolean]("revision_is_identity_reverted_within_90_days")       shouldBe null
    row.getAs[java.lang.Boolean]("revision_is_identity_revert_within_90_days")         shouldBe null
    row.getAs[java.lang.Long]("revision_first_identity_reverting_revision_id")         shouldBe null
    row.getAs[java.lang.Long]("revision_first_identity_reverting_revision_id_within_90_days") shouldBe null
    row.getAs[java.lang.Long]("event_user_revision_count")                             shouldBe null
  }

  it should "classify a bot-by-group performer for page events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-F', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'SomeUser', false, array('bot', 'sysop'), CAST(NULL AS STRING),
          1L, 'A', 0)"""
    )

    pageIncoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") shouldEqual Seq("group")
  }

  it should "classify a bot-by-name performer for page events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-G', '2024-01-15T10:00:01Z',
          'Old', 0,
          1L, CAST(NULL AS BIGINT), 'ClueBot', false, array(), CAST(NULL AS STRING),
          1L, 'New', 0)"""
    )

    pageIncoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") shouldEqual Seq("name")
  }

  it should "look up namespace content flag for page events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-H', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'Talk:Foo', 1)"""
    )

    pageIncoming().collect()(0).getAs[Boolean]("page_namespace_is_content_historical") shouldEqual false
  }

  it should "deduplicate page events on meta.id, keeping the latest meta_dt" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-I', '2024-01-15T10:00:02Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'Page_A', 0),
         ('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-I', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          2L, CAST(NULL AS BIGINT), 'Bob',   false, array(), CAST(NULL AS STRING),
          1L, 'Page_B', 0)"""
    )

    val rows = pageIncoming().collect()
    rows.length shouldEqual 1
    // later meta_dt wins: page_id=1, user=Alice
    rows(0).getAs[String]("event_user_text_historical") shouldEqual "Alice"
  }

  it should "produce one row per distinct meta.id" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move',   '2024-01-15T10:00:00Z', 'uuid-J', '2024-01-15T10:00:01Z',
          'Old_A', 0,
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          10L, 'New_A', 0),
         ('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-K', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING),
          20L, 'Page_B', 0),
         ('dewiki', 'move',   '2024-01-15T10:00:00Z', 'uuid-L', '2024-01-15T10:00:01Z',
          'Old_C', 0,
          1L, CAST(NULL AS BIGINT), 'Bob',   false, array(), CAST(NULL AS STRING),
          30L, 'New_C', 0)"""
    )

    pageIncoming().count() shouldEqual 3
  }

  // ---- revision_deleted_parts from page_change ----

  it should "derive revision_deleted_parts when content is hidden in page_change" in {
    registerEmptyTarget()
    registerNamespaces()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_source AS
         SELECT
           'enwiki'  AS wiki_id,
           'edit'    AS page_change_kind,
           named_struct(
             'rev_id', 101L, 'rev_parent_id', 100L,
             'rev_dt', '2024-01-15T10:00:00Z',
             'rev_size', 500, 'rev_sha1', 'sha-A',
             'is_minor_edit',      false,
             'is_content_visible', false,
             'is_editor_visible',  true,
             'is_comment_visible', true,
             'tags', array(),
             'editor', named_struct(
               'user_id', 1L, 'user_central_id', CAST(NULL AS BIGINT),
               'user_text', 'Alice', 'is_temp', false,
               'groups', array(), 'registration_dt', CAST(NULL AS STRING),
               'edit_count', CAST(NULL AS BIGINT)
             )
           ) AS revision,
           named_struct(
             'revision', named_struct('rev_size', 400L, 'rev_sha1', CAST(NULL AS STRING))
           ) AS prior_state,
           named_struct('page_id', 1L, 'page_title', 'A', 'namespace_id', 0) AS page,
           named_struct('dt', '2024-01-15T10:00:01Z') AS meta,
           2024 AS year, 1 AS month, 15 AS day"""
    )

    incoming().collect()(0).getAs[Seq[String]]("revision_deleted_parts") shouldEqual Seq("text")
  }

  // ---- User events (MERGE 6) ----
  // Positional VALUES: wiki_id, user_change_kind, dt, meta_id, meta_dt,
  //   is_autocreate, user_id, user_central_id, user_text, is_temp, edit_count,
  //   groups, registration_dt, prior_user_id, prior_user_text, prior_groups, prior_is_temp,
  //   performer_user_id, performer_user_central_id, performer_user_text,
  //   performer_is_temp, performer_registration_dt, performer_groups

  "MWHistoryDeltaWriter user events" should "map basic create fields to the target schema" in {
    // Self-registration: performer == user being created
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U1', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 7L, array(), '2020-01-01T00:00:00Z',
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, 99L, 'Alice', false, '2020-01-01T00:00:00Z', array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[String]("event_entity")                 shouldEqual "user"
    row.getAs[String]("event_type")                   shouldEqual "create"
    row.getAs[String]("wiki_id")                      shouldEqual "enwiki"
    // event_user_* = performer (Alice registering herself)
    row.getAs[Long]("event_user_id")                  shouldEqual 42L
    row.getAs[Long]("event_user_central_id")          shouldEqual 99L
    row.getAs[String]("event_user_text_historical")   shouldEqual "Alice"
    // user_* = the user being created
    row.getAs[Long]("user_id")                        shouldEqual 42L
    row.getAs[String]("user_text_historical")         shouldEqual "Alice"
    row.getAs[java.sql.Timestamp]("event_timestamp")  should not be null
  }

  it should "use user.user_text as user_text_historical (post-rename name, not prior name)" in {
    registerUserEventWith(
      """('enwiki', 'rename', '2024-01-15T10:00:00Z', 'uuid-U2', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'NewName', false, 100L, array(), '2020-01-01T00:00:00Z',
          42L, 'OldName', array(), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[String]("user_text_historical")       shouldEqual "NewName"
    row.getAs[String]("event_user_text_historical") shouldEqual "Admin"
  }

  it should "map groups_change to event_type='altergroups'" in {
    registerUserEventWith(
      """('enwiki', 'groups_change', '2024-01-15T10:00:00Z', 'uuid-U3', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 50L, array('sysop'), '2020-01-01T00:00:00Z',
          42L, 'Alice', array(), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array())"""
    )

    userIncoming().collect()(0).getAs[String]("event_type") shouldEqual "altergroups"
  }

  it should "set user_is_created_by_self=true for self-registration" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U5', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, CAST(NULL AS BIGINT), 'Alice', false, CAST(NULL AS STRING), array())"""
    )

    userIncoming().collect()(0).getAs[Boolean]("user_is_created_by_self") shouldEqual true
  }

  it should "set user_is_created_by_self=false when performer_user_id is null (system creation)" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U6b', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS STRING), false, CAST(NULL AS STRING), array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Boolean]("user_is_created_by_self")   shouldEqual false
    row.getAs[Boolean]("user_is_created_by_system") shouldEqual false
  }

  it should "set user_is_created_by_peer=true when an admin creates the account" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U6', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Boolean]("user_is_created_by_self") shouldEqual false
    row.getAs[Boolean]("user_is_created_by_peer") shouldEqual true
  }

  it should "set user_is_created_by_system=true for autocreate events" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U6c', '2024-01-15T10:00:01Z',
          true,
          42L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS STRING), false, CAST(NULL AS STRING), array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Boolean]("user_is_created_by_system") shouldEqual true
    row.getAs[Boolean]("user_is_created_by_self")   shouldEqual false
    row.getAs[Boolean]("user_is_created_by_peer")   shouldEqual false
  }

  it should "set user_is_temporary and user_is_permanent from the changed user" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U8', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, '~2024-TempUser', true, 0L, array('*'), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), true,
          42L, CAST(NULL AS BIGINT), '~2024-TempUser', false, CAST(NULL AS STRING), array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Boolean]("user_is_temporary")        shouldEqual true
    row.getAs[Boolean]("user_is_permanent")        shouldEqual false
    row.getAs[Boolean]("event_user_is_temporary")  shouldEqual false
    row.getAs[Boolean]("event_user_is_permanent")  shouldEqual true
  }

  it should "classify performer as bot by group membership for event_user_is_bot_by_historical" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-U9', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          1L, CAST(NULL AS BIGINT), 'ImportBot', false, CAST(NULL AS STRING), array('bot'))"""
    )

    userIncoming().collect()(0).getAs[Seq[String]]("event_user_is_bot_by_historical") should contain("group")
  }

  it should "classify changed user as bot by user_groups for user_is_bot_by_historical on groups_change" in {
    registerUserEventWith(
      """('enwiki', 'groups_change', '2024-01-15T10:00:00Z', 'uuid-UA', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'MyBot', false, 200L, array('bot'), '2020-01-01T00:00:00Z',
          42L, 'MyBot', array(), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array())"""
    )

    userIncoming().collect()(0).getAs[Seq[String]]("user_is_bot_by_historical") should contain("group")
  }

  it should "set event_user_revision_count to null (performer edit_count not in stream)" in {
    registerUserEventWith(
      """('enwiki', 'rename', '2024-01-15T10:00:00Z', 'uuid-UB', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 1337L, array(), '2020-01-01T00:00:00Z',
          42L, 'OldAlice', array(), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array())"""
    )

    userIncoming().collect()(0).isNullAt(
      userIncoming().schema.fieldIndex("event_user_revision_count")
    ) shouldEqual true
  }

  it should "set all page, revision, and revert fields to null" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-UC', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, CAST(NULL AS BIGINT), 'Alice', false, CAST(NULL AS STRING), array())"""
    )

    val row    = userIncoming().collect()(0)
    val schema = userIncoming().schema
    Seq("page_id", "page_title_historical", "page_namespace_historical",
        "revision_id", "revision_text_bytes",
        "revision_is_identity_reverted", "revision_is_identity_reverted_within_90_days"
    ).foreach { col =>
      row.isNullAt(schema.fieldIndex(col)) shouldEqual true
    }
  }

  it should "deduplicate rows with the same meta_id keeping the latest meta_dt" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-UD', '2024-01-15T10:00:02Z',
          false,
          42L, 99L, 'Alice_v2', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, CAST(NULL AS BIGINT), 'Alice_v2', false, CAST(NULL AS STRING), array()),
         ('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-UD', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice_v1', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, CAST(NULL AS BIGINT), 'Alice_v1', false, CAST(NULL AS STRING), array())"""
    )

    val rows = userIncoming().collect()
    rows.length shouldEqual 1
    rows(0).getAs[String]("event_user_text_historical") shouldEqual "Alice_v2"
  }

  it should "pass through multiple rows with distinct meta_ids" in {
    registerUserEventWith(
      """('enwiki',  'create',        '2024-01-15T10:00:00Z', 'uuid-UE1', '2024-01-15T10:00:01Z',
          false,
          10L, 99L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          10L, CAST(NULL AS BIGINT), 'Alice', false, CAST(NULL AS STRING), array()),
         ('enwiki',  'rename',        '2024-01-15T11:00:00Z', 'uuid-UE2', '2024-01-15T11:00:01Z',
          false,
          20L, 88L, 'NewBob', false, 5L, array(), '2019-01-01T00:00:00Z',
          20L, 'OldBob', array(), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array()),
         ('dewiki',  'groups_change', '2024-01-15T12:00:00Z', 'uuid-UE3', '2024-01-15T12:00:01Z',
          false,
          30L, 77L, 'Carol', false, 50L, array('sysop'), '2018-06-01T00:00:00Z',
          30L, 'Carol', array(), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array())"""
    )

    userIncoming().count() shouldEqual 3
  }

  it should "populate user_groups_historical with post-event groups for altergroups events" in {
    registerUserEventWith(
      """('enwiki', 'groups_change', '2024-01-15T10:00:00Z', 'uuid-UF1', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 50L, array('sysop', 'bot'), '2020-01-01T00:00:00Z',
          42L, 'Alice', array('bot'), false,
          1L, CAST(NULL AS BIGINT), 'Admin', false, CAST(NULL AS STRING), array('sysop'))"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Seq[String]]("user_groups_historical")        shouldEqual Seq("sysop", "bot")
    row.getAs[Seq[String]]("event_user_groups_historical")  shouldEqual Seq("sysop")
  }

  it should "populate user_groups_historical from user_groups for create events (no prior state)" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-UF2', '2024-01-15T10:00:01Z',
          false,
          42L, 99L, 'Alice', false, 0L, array('confirmed'), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, CAST(NULL AS BIGINT), 'Alice', false, CAST(NULL AS STRING), array('confirmed'))"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Seq[String]]("user_groups_historical")        shouldEqual Seq("confirmed")
    row.getAs[Seq[String]]("event_user_groups_historical")  shouldEqual Seq("confirmed")
  }
}
