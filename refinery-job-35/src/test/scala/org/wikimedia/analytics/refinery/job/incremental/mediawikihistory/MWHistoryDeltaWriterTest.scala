package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Tests for MWHistoryDeltaWriter.
 *
 * All tests run the CTE chain (buildIncomingSQL + SELECT * FROM incoming) against
 * synthetic temp views, without Iceberg. The MERGE itself is not tested here — the
 * SQL is structurally identical to what Iceberg executes.
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
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  val params: MWHistoryDeltaWriter.Params = MWHistoryDeltaWriter.Params(
    pageChangeTable    = "test_source",
    targetTable        = "test_target",
    namespacesTable    = "test_namespaces",
    namespacesSnapshot = "2024-12",
    tagsTable          = "test_tags",
    visibilityTable    = "test_visibility",
    userChangeTable    = "test_user_source",
    year = 2024, month = 1, day = 15
  )

  def incoming()         = spark.sql(MWHistoryDeltaWriter.buildIncomingSQL(params)      + "\nSELECT * FROM incoming")
  def backPatch()        = spark.sql(MWHistoryDeltaWriter.buildIncomingSQL(params)      + "\nSELECT * FROM back_patch")
  def latestTags()       = spark.sql(MWHistoryDeltaWriter.buildTagsCteSQL(params)       + "\nSELECT * FROM latest_tags")
  def latestVisibility() = spark.sql(MWHistoryDeltaWriter.buildVisibilityCteSQL(params) + "\nSELECT * FROM latest_visibility")
  def pageIncoming()     = spark.sql(MWHistoryDeltaWriter.buildPageIncomingSQL(params)  + "\nSELECT * FROM page_incoming")
  def userIncoming()     = spark.sql(MWHistoryDeltaWriter.buildUserIncomingSQL(params)  + "\nSELECT * FROM user_incoming")

  /** Empty target — only needed for the revert_seed CTE. */
  def registerEmptyTarget(): Unit =
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT
           CAST(NULL AS STRING)    AS wiki_id,
           CAST(NULL AS BIGINT)    AS revision_id,
           CAST(NULL AS STRING)    AS revision_text_sha1,
           CAST(NULL AS BIGINT)    AS revision_parent_id,
           CAST(NULL AS BIGINT)    AS page_id,
           CAST(NULL AS TIMESTAMP) AS event_timestamp,
           CAST(NULL AS BOOLEAN)   AS revision_is_identity_reverted
         WHERE 1 = 0"""
    )

  def registerNamespaces(): Unit =
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_namespaces AS
         SELECT 'enwiki' AS dbname, 0 AS namespace, 1 AS namespace_is_content, '2024-12' AS snapshot
         UNION ALL
         SELECT 'enwiki', 1, 0, '2024-12'
         UNION ALL
         SELECT 'enwiki', 2, 0, '2024-12'
         UNION ALL
         SELECT 'enwiki', 4, 0, '2024-12'
         UNION ALL
         SELECT 'dewiki', 0, 1, '2024-12'"""
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
      "--page_change_table",  "event.mediawiki_page_change",
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
    p.pageChangeTable    shouldEqual "event.mediawiki_page_change"
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
  //
  // The daily writer's revert algorithm is semantically equivalent to the monthly
  // implementation in DenormalizedRevisionsBuilder. Key consequences exercised below:
  //   - A sha-series base is NOT reverted by its own series — its content survives
  //     when the reverter restores it. Only intermediate (different-sha) revisions
  //     inside the base→reverter window are reverted.
  //   - A reverter at rank ≥ 2 is is_revert=TRUE; it is_reverted=TRUE only when the
  //     next pending reverter has a DIFFERENT base (the "wider revert" branch in
  //     DenormalizedRevisionsBuilder.updateRevisionAndReverts:266–276).
  //   - Same-sha compound sequences (R1=A, R2=A, R3=A) leave each rank≥2 reverter as
  //     is_revert only — the next pending head has the SAME base, so no is_reverted.

  it should "detect a wider revert: intermediate different-sha revision reverted by sha-series reverter" in {
    registerEmptyTarget()
    registerNamespaces()
    // Canonical A → B → A pattern: R101 sets sha-X, R102 changes to sha-Y, R103 restores sha-X.
    // Per monthly: R102 (intermediate) is reverted by R103. R101 (base) is NOT reverted.
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L,   '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit', 102L, 101L, '2024-01-15T10:30:00Z', 600, 'sha-Y', 500L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:30:01Z'),
         ('enwiki', 'edit', 103L, 102L, '2024-01-15T11:00:00Z', 500, 'sha-X', 600L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T11:00:01Z')"""
    )

    val rows = incoming().collect().sortBy(_.getAs[Long]("revision_id"))

    // R101 (sha-X base): fresh revision with sha1, no reverter in batch → FALSE (not null).
    rows(0).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual false
    rows(0).getAs[Boolean]("revision_is_identity_revert")                shouldEqual false

    // R102 (sha-Y intermediate): reverted by R103 — head of pending reverters at R102's processing.
    rows(1).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual true
    rows(1).getAs[Long]("revision_first_identity_reverting_revision_id") shouldEqual 103L
    rows(1).getAs[Long]("revision_seconds_to_identity_revert")           shouldEqual 1800L
    rows(1).getAs[Boolean]("revision_is_identity_revert")                shouldEqual false

    // R103 (sha-X reverter): is_revert. No different-base window contains it → not reverted.
    rows(2).getAs[Boolean]("revision_is_identity_revert")                shouldEqual true
    rows(2).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual false
  }

  it should "leave same-sha compound rank>=2 reverters as is_revert only (no wider revert applies)" in {
    registerEmptyTarget()
    registerNamespaces()
    // R101, R105, R107 all sha-X with no different-sha intermediate. Per monthly's
    // "same wider base" branch (DenormalizedRevisionsBuilder.scala:266), R105's next
    // pending head is R107 with the SAME base — only is_revert, no is_reverted.
    // R101 (base) is never reverted by its own sha-series.
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L,   '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit', 105L, 104L, '2024-01-15T12:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T12:00:01Z'),
         ('enwiki', 'edit', 107L, 106L, '2024-01-15T13:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T13:00:01Z')"""
    )

    val rows = incoming().collect().sortBy(_.getAs[Long]("revision_id"))

    // R101 (rank=1, base): fresh, sha1 present, no reverter → FALSE.
    rows(0).getAs[Boolean]("revision_is_identity_reverted")           shouldEqual false
    rows(0).getAs[Boolean]("revision_is_identity_revert")             shouldEqual false

    // R105 (rank=2, same-sha next head): is_revert=TRUE; fresh with sha1 so reverted=FALSE.
    rows(1).getAs[Boolean]("revision_is_identity_revert")             shouldEqual true
    rows(1).getAs[Boolean]("revision_is_identity_reverted")           shouldEqual false

    // R107 (rank=3, no further reverter): is_revert only.
    rows(2).getAs[Boolean]("revision_is_identity_revert")             shouldEqual true
    rows(2).getAs[Boolean]("revision_is_identity_reverted")           shouldEqual false
  }

  it should "mark a reverter as is_revert AND is_reverted when the next head has a different base" in {
    registerEmptyTarget()
    registerNamespaces()
    // R201=sha-A, R202=sha-B, R203=sha-A, R204=sha-B on one page. Per monthly:
    //   R201 (A-base): not reverted (R203 not pending at R201's processing).
    //   R202 (B-base): reverted by R203 — pending head of A's wider window.
    //   R203 (A-reverter, rank=2): is_revert; next head R204 has DIFFERENT base → also is_reverted.
    //   R204 (B-reverter, rank=2): is_revert; queue empty after pop → not reverted.
    registerSourceWith(
      """('enwiki', 'edit', 201L, 0L,   '2024-01-15T09:00:00Z', 500, 'sha-A', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T09:00:01Z'),
         ('enwiki', 'edit', 202L, 201L, '2024-01-15T10:00:00Z', 600, 'sha-B', 500L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit', 203L, 202L, '2024-01-15T11:00:00Z', 500, 'sha-A', 600L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T11:00:01Z'),
         ('enwiki', 'edit', 204L, 203L, '2024-01-15T12:00:00Z', 600, 'sha-B', 500L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T12:00:01Z')"""
    )

    val rows = incoming().collect().sortBy(_.getAs[Long]("revision_id"))

    rows(0).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual false
    rows(0).getAs[Boolean]("revision_is_identity_revert")                shouldEqual false

    rows(1).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual true
    rows(1).getAs[Long]("revision_first_identity_reverting_revision_id") shouldEqual 203L

    rows(2).getAs[Boolean]("revision_is_identity_revert")                shouldEqual true
    rows(2).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual true
    rows(2).getAs[Long]("revision_first_identity_reverting_revision_id") shouldEqual 204L

    rows(3).getAs[Boolean]("revision_is_identity_revert")                shouldEqual true
    rows(3).getAs[Boolean]("revision_is_identity_reverted")              shouldEqual false
  }

  // Canonical integration test: ported directly from
  // TestDenormalizedRevisionsBuilder.scala "should correctly update revision in case
  // of nested reverts" (line 594). Strongest test of semantic equivalence with monthly.
  it should "match monthly's nested-reverts golden case (9 revs, 3 sha-series with reverters)" in {
    registerEmptyTarget()
    registerNamespaces()
    // page=1 sha-series (the rev ids match the monthly test's revIds for easy comparison):
    //   s1: rev1 (base), rev6 (reverter)
    //   s2: rev2 (base), rev4 (reverter)
    //   s3: rev3 (base), rev8 (reverter)
    //   s5, s7, s9: single occurrences (no reverters; rank=1)
    registerSourceWith(
      """('enwiki', 'edit', 1L, 0L, '2024-01-15T01:00:00Z', 100, 's1', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T01:00:01Z'),
         ('enwiki', 'edit', 2L, 1L, '2024-01-15T02:00:00Z', 100, 's2', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T02:00:01Z'),
         ('enwiki', 'edit', 3L, 2L, '2024-01-15T03:00:00Z', 100, 's3', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T03:00:01Z'),
         ('enwiki', 'edit', 4L, 3L, '2024-01-15T04:00:00Z', 100, 's2', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T04:00:01Z'),
         ('enwiki', 'edit', 5L, 4L, '2024-01-15T05:00:00Z', 100, 's5', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T05:00:01Z'),
         ('enwiki', 'edit', 6L, 5L, '2024-01-15T06:00:00Z', 100, 's1', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T06:00:01Z'),
         ('enwiki', 'edit', 7L, 6L, '2024-01-15T07:00:00Z', 100, 's7', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T07:00:01Z'),
         ('enwiki', 'edit', 8L, 7L, '2024-01-15T08:00:00Z', 100, 's3', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T08:00:01Z'),
         ('enwiki', 'edit', 9L, 8L, '2024-01-15T09:00:00Z', 100, 's9', 100L,                 1L, CAST(NULL AS BIGINT), 'U', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T09:00:01Z')"""
    )

    val rows = incoming().collect().sortBy(_.getAs[Long]("revision_id"))

    def check(idx: Int, isRevert: Option[Boolean], isReverted: Option[Boolean],
              revertingId: Option[Long], secondsToRevert: Option[Long]): Unit = {
      val row = rows(idx)
      val gotIsRevert       = Option(row.getAs[java.lang.Boolean]("revision_is_identity_revert")).map(_.booleanValue)
      val gotIsReverted     = Option(row.getAs[java.lang.Boolean]("revision_is_identity_reverted")).map(_.booleanValue)
      val gotRevertingId    = Option(row.getAs[java.lang.Long]("revision_first_identity_reverting_revision_id")).map(_.longValue)
      val gotSecondsToRevert = Option(row.getAs[java.lang.Long]("revision_seconds_to_identity_revert")).map(_.longValue)
      gotIsRevert        shouldEqual isRevert
      gotIsReverted      shouldEqual isReverted
      gotRevertingId     shouldEqual revertingId
      gotSecondsToRevert shouldEqual secondsToRevert
    }

    // Expectations match TestDenormalizedRevisionsBuilder.scala:621–636 (translated:
    // monthly's 1-day intervals scaled to 1 hour each, giving 3600s per hour).
    //   rev1: no annotation.
    //   rev2: reverted by rev6, 4 hours.
    //   rev3: reverted by rev4, 1 hour.
    //   rev4: is_revert + reverted by rev6, 2 hours.
    //   rev5: reverted by rev6, 1 hour.
    //   rev6: is_revert + reverted by rev8, 2 hours.
    //   rev7: reverted by rev8, 1 hour.
    //   rev8: is_revert (no further reverter).
    //   rev9: no annotation.
    // Fresh revisions with sha1 and no reverter detected get FALSE (not null).
    check(0, Some(false), Some(false), None,    None)
    check(1, Some(false), Some(true),  Some(6L), Some(4 * 3600L))
    check(2, Some(false), Some(true),  Some(4L), Some(1 * 3600L))
    check(3, Some(true),  Some(true),  Some(6L), Some(2 * 3600L))
    check(4, Some(false), Some(true),  Some(6L), Some(1 * 3600L))
    check(5, Some(true),  Some(true),  Some(8L), Some(2 * 3600L))
    check(6, Some(false), Some(true),  Some(8L), Some(1 * 3600L))
    check(7, Some(true),  Some(false), None,    None)
    check(8, Some(false), Some(false), None,    None)
  }

  it should "detect a revert even when the seed sha1 is more than 90 days old" in {
    registerNamespaces()
    // Full-table scan: rev 99 from 2023-10-01 (>90d before 2024-01-15) is included.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2023-10-01T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 103L, 102L, '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:02Z')"""
    )

    val rows = incoming().collect()
    rows.length shouldEqual 1
    // Full-table scan finds the old seed — revert IS detected
    rows(0).getAs[Boolean]("revision_is_identity_revert") shouldEqual true
  }

  it should "detect a cross-day revert using the seed from the target table" in {
    registerNamespaces()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-14T16:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T02:00:00Z', 300, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T02:00:01Z')"""
    )

    val row = incoming().collect()(0)
    // The incoming revision repeats the sha1 of a seed row — it IS a revert
    row.getAs[Boolean]("revision_is_identity_revert")    shouldEqual true
    // It has not itself been reverted in this batch; fresh with sha1 → FALSE
    row.getAs[Boolean]("revision_is_identity_reverted")  shouldEqual false
  }

  it should "not self-revert a revision that is already in the target with the same sha1 (rerun safety)" in {
    registerNamespaces()
    // Simulates a rerun: rev 101 already committed to the target as source='events'.
    // On rerun, rev 101 appears in both the incoming batch AND the seed.
    // Without the ANTI JOIN guard, rev 101 would get sha1_rank=1 (seed) and
    // sha1_rank=2 (incoming), detecting itself as a revert with seconds=0.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 101L AS revision_id,
                'sha-X' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-15T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted") shouldEqual false
    row.getAs[Boolean]("revision_is_identity_revert")   shouldEqual false
  }

  it should "set revert fields to false for fresh revisions with sha1 when no revert detected" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 500, 'sha-unique',
          400L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[Boolean]("revision_is_identity_reverted") shouldEqual false
    row.getAs[Boolean]("revision_is_identity_revert")   shouldEqual false
  }

  // ---- Back-patch CTE ----
  //
  // Back-patch fires for seed rows whose monthly-equivalent "first reverter" arrives
  // in today's incoming AND whose is_reverted predicate would be TRUE (non-reverter,
  // OR reverter whose next pending head has a different base). A sha-series base
  // whose only same-sha reverter is incoming is NOT back-patched — its content
  // survived; only the intermediate (different-sha) rows inside the window are.

  it should "back-patch the intermediate seed row when today's incoming brings the sha-series reverter" in {
    registerNamespaces()
    // Seed: R99 (sha-A, base) + R100 (sha-B, intermediate). Incoming: R101 (sha-A, reverter).
    // R100 falls inside R99→R101 window → back-patched. R99 (the base) is NOT patched.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L  AS revision_id,
                'sha-A' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-14T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted
         UNION ALL
         SELECT 'enwiki', 100L, 'sha-B', 99L, 1L,
                to_timestamp('2024-01-14T11:00:00Z'),
                CAST(NULL AS BOOLEAN)"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T02:00:00Z', 300, 'sha-A', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T02:00:01Z')"""
    )

    val rows = backPatch().collect()
    rows.length                                   shouldEqual 1
    rows(0).getAs[Long]("target_revision_id")     shouldEqual 100L
    rows(0).getAs[Long]("first_reverting_rev_id") shouldEqual 101L
    // R101 ts - R100 ts = 2024-01-15T02:00 - 2024-01-14T11:00 = 15 hours
    rows(0).getAs[Long]("seconds_to_revert")      shouldEqual 15 * 3600L
  }

  it should "back-patch an intermediate seed row even when the sha-series base seed is more than 90 days old" in {
    registerNamespaces()
    // Same pattern, but the A-base is from 2023-10-01. The page-scoped seed reads it
    // anyway — the daily writer no longer drops old rows from the seed when they share
    // a page with incoming.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L  AS revision_id,
                'sha-A' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2023-10-01T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted
         UNION ALL
         SELECT 'enwiki', 100L, 'sha-B', 99L, 1L,
                to_timestamp('2024-01-14T11:00:00Z'),
                CAST(NULL AS BOOLEAN)"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 103L, 102L, '2024-01-15T10:00:00Z', 500, 'sha-A', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:02Z')"""
    )

    val rows = backPatch().collect()
    rows.length                                   shouldEqual 1
    rows(0).getAs[Long]("target_revision_id")     shouldEqual 100L
    rows(0).getAs[Long]("first_reverting_rev_id") shouldEqual 103L
  }

  it should "not back-patch a sha-series base whose only same-sha reverter arrives today (no different-base window)" in {
    registerNamespaces()
    // Seed: R99 only (sha-X base). Incoming: R103 (sha-X reverter). No intermediate.
    // Per monthly the base is never marked reverted by its own series → no back-patch.
    // This was the OVER-patching case under the previous sha1_rank+1 algorithm.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-14T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 103L, 99L,  '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:02Z')"""
    )

    backPatch().count() shouldEqual 0
  }

  it should "produce no back_patch row when the incoming revision is already in the target (rerun safety)" in {
    registerNamespaces()
    // Simulates a rerun: rev 101 already in the target. ANTI JOIN drops it from seed
    // so it can't be both seed and incoming for the ranking.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 101L AS revision_id,
                'sha-X' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-15T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 101L, 100L, '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    backPatch().count() shouldEqual 0
  }

  it should "produce no back_patch row when both reverted and reverting revisions are incoming" in {
    registerEmptyTarget()
    registerNamespaces()
    // Canonical A→B→A pattern, all incoming. The main writer handles the annotations;
    // back-patch has no seed to update.
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L,   '2024-01-15T10:00:00Z', 500, 'sha-A', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T10:00:01Z'),
         ('enwiki', 'edit', 102L, 101L, '2024-01-15T10:30:00Z', 600, 'sha-B', 500L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T10:30:01Z'),
         ('enwiki', 'edit', 103L, 102L, '2024-01-15T11:00:00Z', 500, 'sha-A', 600L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-15T11:00:01Z')"""
    )

    backPatch().count() shouldEqual 0
  }

  it should "back-patch a seed reverter when today's incoming brings a different-base wider reverter" in {
    registerNamespaces()
    // Seed:  R99  (sha-A, A-base, existing_reverted=NULL)              t=2024-01-13
    //        R100 (sha-B, B-base, existing_reverted=TRUE by R101)      t=2024-01-14
    //        R101 (sha-A, A-reverter rank=2, existing_reverted=NULL)   t=2024-01-14T12:00
    // Incoming: R102 (sha-B, B-reverter)  t=2024-01-15
    //
    // Per monthly: R101 (a reverter at rank=2) is is_revert AND ALSO reverted by R102
    // — the next pending head has a different base. R100 was already reverted by R101
    // in a previous run (R101 is in seed, not incoming) and the seed annotation is
    // TRUE, so the annotation-aware filter skips R100 in the back-patch candidates.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L  AS revision_id, 'sha-A' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id, to_timestamp('2024-01-13T10:00:00Z') AS event_timestamp, CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted
         UNION ALL
         SELECT 'enwiki', 100L, 'sha-B', 99L,  1L, to_timestamp('2024-01-14T10:00:00Z'), TRUE
         UNION ALL
         SELECT 'enwiki', 101L, 'sha-A', 100L, 1L, to_timestamp('2024-01-14T12:00:00Z'), CAST(NULL AS BOOLEAN)"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 102L, 101L, '2024-01-15T10:00:00Z', 600, 'sha-B', 500L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    val rows = backPatch().collect()
    rows.length                                   shouldEqual 1
    rows(0).getAs[Long]("target_revision_id")     shouldEqual 101L
    rows(0).getAs[Long]("first_reverting_rev_id") shouldEqual 102L
  }

  it should "not re-patch a seed row whose first reverter was already in the target from a prior run" in {
    registerNamespaces()
    // Seed has the full history of an A→B→A revert (R99, R100, R101 all in target).
    // R100 carries existing_reverted=TRUE from the prior run (its reverter R101 is in
    // seed). Today's incoming brings R103 (sha-X) — no overlap with the A or B series.
    // The annotation-aware filter skips R100; R99 and R101 produce no candidates from
    // the unrelated R103, so back_patch emits nothing.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L  AS revision_id, 'sha-A' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id, to_timestamp('2024-01-12T10:00:00Z') AS event_timestamp, CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted
         UNION ALL
         SELECT 'enwiki', 100L, 'sha-B', 99L,  1L, to_timestamp('2024-01-13T10:00:00Z'), TRUE
         UNION ALL
         SELECT 'enwiki', 101L, 'sha-A', 100L, 1L, to_timestamp('2024-01-14T10:00:00Z'), CAST(NULL AS BOOLEAN)"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 103L, 101L, '2024-01-15T10:00:00Z', 700, 'sha-X', 500L, 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    backPatch().count() shouldEqual 0
  }

  // ---- Revert bug-fix tests ----

  it should "not flag a sha-series rank=2 row as a revert when its parent IS the sha-series base (no-op duplicate)" in {
    registerNamespaces()
    // When a rank>=2 row R has revision_parent_id pointing at the rank=1 base of its
    // sha-series, R's content already equals its parent's content — a no-op duplicate
    // (null edit, page-merge artifact, replay-emitted duplicate). It isn't restoring
    // any earlier state, so it shouldn't be flagged as a revert nor should it back-patch
    // the base.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                'sha-X' AS revision_text_sha1, CAST(NULL AS BIGINT) AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-14T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    // Incoming R100: same sha-X as seed R99, parent=99 (R99 is the sha-X series base).
    registerSourceWith(
      """('enwiki', 'edit', 100L, 99L, '2024-01-15T10:00:00Z', 500, 'sha-X', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    // R99 not back-patched: R100 is a no-op (parent = own_base) and is filtered from
    // reverters_with_base entirely, so there's no candidate to flag R99.
    backPatch().count() shouldEqual 0
    val row = incoming().collect()(0)
    // R100 is also not flagged as a revert (is_real_reverter = FALSE); fresh + sha1 → FALSE.
    row.getAs[Boolean]("revision_is_identity_reverted") shouldEqual false
    row.getAs[Boolean]("revision_is_identity_revert")   shouldEqual false
  }

  it should "exclude NULL revision_text_sha1 rows from revert detection (monthly-bug workaround)" in {
    registerNamespaces()
    // Two suppressed-text revisions on the same page would partition together under
    // PARTITION BY (page_id, NULL) and falsely rank=1/rank=2 if not filtered. Filter is
    // applied early in both revert_seed and revert_candidates.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW test_target AS
         SELECT 'enwiki' AS wiki_id, 99L AS revision_id,
                CAST(NULL AS STRING) AS revision_text_sha1, 98L AS revision_parent_id, 1L AS page_id,
                to_timestamp('2024-01-14T10:00:00Z') AS event_timestamp,
                CAST(NULL AS BOOLEAN) AS revision_is_identity_reverted"""
    )
    registerSourceWith(
      """('enwiki', 'edit', 100L, 99L, '2024-01-15T10:00:00Z', 500, CAST(NULL AS STRING), CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'A', '2024-01-15T10:00:01Z')"""
    )

    // Seed R99 not back-patched, incoming R100 not flagged.
    backPatch().count() shouldEqual 0
    val row = incoming().collect()(0)
    row.getAs[java.lang.Boolean]("revision_is_identity_reverted") shouldBe null
    row.getAs[java.lang.Boolean]("revision_is_identity_revert")   shouldBe null
  }

  it should "not flag a rank>=3 sha-series row as a revert when parent is the immediately prior same-sha row (Joseph's #12 no-op chain)" in {
    registerEmptyTarget()
    registerNamespaces()
    // Mirrors Joseph's new test data in TestDenormalizedRevisionsBuilder (Gerrit 1295404):
    //   #9  sha-A base
    //   #10 sha-B intermediate
    //   #11 sha-A rank=2, parent=0 → real reverter (parent ≠ prev=#9)
    //   #12 sha-A rank=3, parent=#11 → NO-OP duplicate of #11 (parent = prev in sha-A series)
    // Expected: #11 marked is_revert; #10 marked is_reverted by #11; #12 NOT flagged as
    // a revert; #9 not back-patched (base of its own series).
    registerSourceWith(
      """('enwiki', 'create', 9L,  0L,  '2023-12-20T00:00:00Z', 100, 'sha-A', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2023-12-20T00:00:01Z'),
         ('enwiki', 'edit',   10L, 9L,  '2023-12-21T00:00:00Z', 200, 'sha-B', 100L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2023-12-21T00:00:01Z'),
         ('enwiki', 'edit',   11L, 0L,  '2024-01-10T00:00:00Z', 100, 'sha-A', 200L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-10T00:00:01Z'),
         ('enwiki', 'edit',   12L, 11L, '2024-01-10T01:00:00Z', 100, 'sha-A', 100L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-10T01:00:01Z')"""
    )

    val byId = incoming().collect().map(r => r.getAs[Long]("revision_id") -> r).toMap

    byId(11L).getAs[java.lang.Boolean]("revision_is_identity_revert")   shouldEqual true
    byId(12L).getAs[java.lang.Boolean]("revision_is_identity_revert")   shouldBe null
    byId(12L).getAs[java.lang.Boolean]("revision_is_identity_reverted") shouldBe null
    byId(10L).getAs[java.lang.Boolean]("revision_is_identity_reverted") shouldEqual true
    byId(10L).getAs[Long]("revision_first_identity_reverting_revision_id") shouldEqual 11L
    byId(9L).getAs[java.lang.Boolean]("revision_is_identity_reverted")  shouldBe null
  }

  it should "detect a wider revert across same-base reverters in the inner queue (Joseph's complex wider revert)" in {
    registerEmptyTarget()
    registerNamespaces()
    // Mirrors Joseph's new "different wider revert - complex" test (Gerrit 1295404).
    // The fix changes the wider-revert check from "next reverter has same base" to
    // "first DIFFERENT-base reverter in the remaining queue". Scenario:
    //   rev 1 sha-S1 base; rev 2 sha-S2 base.
    //   rev 3 sha-S2 rank=2 (real reverter; base=rev 2 = own_base).
    //   rev 4 sha-S2 rank=3 (real reverter; SAME base as rev 3) — ts immediately after rev 3.
    //   rev 5 sha-S1 rank=2 (real reverter; DIFFERENT base from rev 3) — ts after rev 4.
    // Pending reverters at rev 3: {rev 4 (same base), rev 5 (diff base)}.
    // Old algorithm: global MIN = rev 4 → same base → "not reverted" (BUG).
    // New algorithm: diff-base MIN = rev 5 → "reverted by rev 5".
    registerSourceWith(
      """('enwiki', 'create', 1L, 0L, '2024-01-01T00:00:00Z', 100, 'sha-S1', CAST(NULL AS BIGINT), 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-01T00:00:01Z'),
         ('enwiki', 'edit',   2L, 1L, '2024-01-02T00:00:00Z', 200, 'sha-S2', 100L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-02T00:00:01Z'),
         ('enwiki', 'edit',   3L, 1L, '2024-01-03T00:00:00Z', 200, 'sha-S2', 100L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-03T00:00:01Z'),
         ('enwiki', 'edit',   4L, 1L, '2024-01-04T00:00:00Z', 200, 'sha-S2', 100L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-04T00:00:01Z'),
         ('enwiki', 'edit',   5L, 2L, '2024-01-05T00:00:00Z', 100, 'sha-S1', 200L,                 1L, CAST(NULL AS BIGINT), 'Alice', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT), 0, 1L, 'P', '2024-01-05T00:00:01Z')"""
    )

    val byId = incoming().collect().map(r => r.getAs[Long]("revision_id") -> r).toMap

    byId(3L).getAs[java.lang.Boolean]("revision_is_identity_revert")   shouldEqual true
    byId(3L).getAs[java.lang.Boolean]("revision_is_identity_reverted") shouldEqual true
    byId(3L).getAs[Long]("revision_first_identity_reverting_revision_id") shouldEqual 5L
    // Seconds = (2024-01-05) - (2024-01-03) = 172800.
    byId(3L).getAs[Long]("revision_seconds_to_identity_revert") shouldEqual 172800L
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
    row.getAs[java.lang.Long]("revision_first_identity_reverting_revision_id")         shouldBe null
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

  // ---- User events (MERGE 7) ----
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
        "revision_is_identity_reverted", "revision_is_identity_revert"
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

  // ---- user_central_id (MERGE 7) ----

  it should "populate user_central_id from user.user_central_id for user events" in {
    registerUserEventWith(
      """('enwiki', 'create', '2024-01-15T10:00:00Z', 'uuid-UC1', '2024-01-15T10:00:01Z',
          false,
          42L, 12345L, 'Alice', false, 0L, array(), CAST(NULL AS STRING),
          CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), false,
          42L, CAST(NULL AS BIGINT), 'Alice', false, CAST(NULL AS STRING), array())"""
    )

    val row = userIncoming().collect()(0)
    row.getAs[Long]("user_central_id") shouldEqual 12345L
  }

  // ---- event_user_is_cross_wiki ----

  "MWHistoryDeltaWriter revision events" should "set event_user_is_cross_wiki=false for regular named users" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), 42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING), 7L,
          0, 1L, 'Page', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[Boolean]("event_user_is_cross_wiki") shouldEqual false
  }

  it should "set event_user_is_cross_wiki=true for anon users with >-format username" in {
    registerEmptyTarget()
    registerNamespaces()
    registerSourceWith(
      """('enwiki', 'edit', 101L, 0L, '2024-01-15T10:00:00Z', 100, 'sha',
          CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), '192.0.2.1>ExampleUser', false, array(), CAST(NULL AS STRING), CAST(NULL AS BIGINT),
          0, 1L, 'Page', '2024-01-15T10:00:01Z')"""
    )

    val row = incoming().collect()(0)
    row.getAs[Boolean]("event_user_is_cross_wiki") shouldEqual true
  }

  "MWHistoryDeltaWriter page events" should "set event_user_is_cross_wiki=false for regular named users" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-CW1', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'New_Title', 0)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[Boolean]("event_user_is_cross_wiki") shouldEqual false
  }

  // ---- page_is_deleted (MERGE 5) ----

  it should "set page_is_deleted=true for delete events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-PD1', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'Page', 0)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[Boolean]("page_is_deleted") shouldEqual true
  }

  it should "set page_is_deleted=false for undelete events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'undelete', '2024-01-15T10:00:00Z', 'uuid-PD2', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'Page', 0)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[Boolean]("page_is_deleted") shouldEqual false
  }

  it should "set page_is_deleted=false for move events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-PD3', '2024-01-15T10:00:01Z',
          'Old_Title', 0,
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          1L, 'New_Title', 0)"""
    )

    val row = pageIncoming().collect()(0)
    row.getAs[Boolean]("page_is_deleted") shouldEqual false
  }

  // ---- M6: page-deletion back-patch (buildPageDeletionBackpatchSQL) ----

  "MWHistoryDeltaWriter.buildPageDeletionBackpatchSQL" should "include delete events in page_deletion_events with is_delete=true" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'delete', '2024-01-15T10:00:00Z', 'uuid-M6A', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          77L, 'Page', 0)"""
    )

    val sql = MWHistoryDeltaWriter.buildPageDeletionBackpatchSQL(params)
    val cteOnly = sql.substring(0, sql.lastIndexOf("MERGE INTO"))
    val rows = spark.sql(cteOnly + "\nSELECT * FROM page_deletion_events").collect()
    rows.length shouldEqual 1
    rows(0).getAs[Long]("page_id")       shouldEqual 77L
    rows(0).getAs[Boolean]("is_delete")  shouldEqual true
  }

  it should "include undelete events in page_deletion_events with is_delete=false" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'undelete', '2024-01-15T10:00:00Z', 'uuid-M6B', '2024-01-15T10:00:01Z',
          CAST(NULL AS STRING), CAST(NULL AS INT),
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          77L, 'Page', 0)"""
    )

    val sql = MWHistoryDeltaWriter.buildPageDeletionBackpatchSQL(params)
    val cteOnly = sql.substring(0, sql.lastIndexOf("MERGE INTO"))
    val rows = spark.sql(cteOnly + "\nSELECT * FROM page_deletion_events").collect()
    rows.length shouldEqual 1
    rows(0).getAs[Boolean]("is_delete") shouldEqual false
  }

  it should "exclude move events from page_deletion_events" in {
    registerNamespaces()
    registerPageEventWith(
      """('enwiki', 'move', '2024-01-15T10:00:00Z', 'uuid-M6C', '2024-01-15T10:00:01Z',
          'Old', 0,
          42L, 999L, 'Alice', false, array(), CAST(NULL AS STRING),
          77L, 'New', 0)"""
    )

    val sql = MWHistoryDeltaWriter.buildPageDeletionBackpatchSQL(params)
    val cteOnly = sql.substring(0, sql.lastIndexOf("MERGE INTO"))
    spark.sql(cteOnly + "\nSELECT * FROM page_deletion_events").count() shouldEqual 0
  }

  // ---- WAP (Write-Audit-Publish) ----

  "MWHistoryDeltaWriter.parseArgs" should "parse --catalog and --iceberg_branch" in {
    val p = MWHistoryDeltaWriter.parseArgs(Array(
      "--page_change_table",  "event.mediawiki_page_change",
      "--target_table",       "wmf_mediawiki.mediawiki_history_incremental_v1",
      "--namespaces_table",   "wmf_raw.mediawiki_project_namespace_map",
      "--namespaces_snapshot","2024-12",
      "--tags_table",         "event.mediawiki_revision_tags_change",
      "--visibility_table",   "event.mediawiki_revision_visibility_change",
      "--user_change_table",  "event.mediawiki_user_change",
      "--year",  "2024", "--month", "1", "--day", "15",
      "--catalog", "spark_catalog",
      "--iceberg_branch", "daily_2024_01_15"
    ))
    p.catalog       shouldEqual "spark_catalog"
    p.icebergBranch shouldEqual "daily_2024_01_15"
  }

  it should "default catalog and icebergBranch to empty string when absent" in {
    val p = MWHistoryDeltaWriter.parseArgs(Array(
      "--page_change_table",  "event.mediawiki_page_change",
      "--target_table",       "wmf_mediawiki.mediawiki_history_incremental_v1",
      "--namespaces_table",   "wmf_raw.mediawiki_project_namespace_map",
      "--namespaces_snapshot","2024-12",
      "--tags_table",         "event.mediawiki_revision_tags_change",
      "--visibility_table",   "event.mediawiki_revision_visibility_change",
      "--user_change_table",  "event.mediawiki_user_change",
      "--year",  "2024", "--month", "1", "--day", "15"
    ))
    p.catalog       shouldEqual ""
    p.icebergBranch shouldEqual ""
  }

  "MWHistoryDeltaWriter SQL builders" should "use branch-scoped table for MERGE INTO when icebergBranch is set" in {
    val p = params.copy(catalog = "local", icebergBranch = "daily_2024_01_15")
    MWHistoryDeltaWriter.buildMergeSQL(p)                           should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildBackPatchSQL(p)                       should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildTagsMergeSQL(p)                       should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildVisibilityMergeSQL(p)                 should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildPageEventMergeSQL(p)                  should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildPageDeletionBackpatchSQL(p)           should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildUserEventMergeSQL(p)                  should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
    MWHistoryDeltaWriter.buildUserCreationProvenanceBackfillSQL(p)  should include ("MERGE INTO local.test_target.branch_daily_2024_01_15")
  }

  it should "use branch-scoped table for the revert_seed FROM clause when icebergBranch is set" in {
    val p = params.copy(catalog = "local", icebergBranch = "daily_2024_01_15")
    MWHistoryDeltaWriter.buildIncomingSQL(p) should include ("FROM local.test_target.branch_daily_2024_01_15 t")
  }

  it should "use branch-scoped table for user_provenance FROM clause when icebergBranch is set" in {
    val p = params.copy(catalog = "local", icebergBranch = "daily_2024_01_15")
    MWHistoryDeltaWriter.buildUserCreationProvenanceBackfillSQL(p) should include ("FROM local.test_target.branch_daily_2024_01_15")
  }

  it should "use plain table name for all SQL when icebergBranch is empty" in {
    MWHistoryDeltaWriter.buildMergeSQL(params)                          should not include ".branch_"
    MWHistoryDeltaWriter.buildIncomingSQL(params)                       should not include ".branch_"
    MWHistoryDeltaWriter.buildUserCreationProvenanceBackfillSQL(params) should not include ".branch_"
  }
}
