package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Daily delta writer for mediawiki_history_incremental_v1.
 *
 * Reads mediawiki.page_change events for one calendar day, maps them to the
 * narrow schema, computes revert detection via the wider-revert algorithm, applies
 * namespace-content and bot classification, then MERGE INTOs the Iceberg target.
 *
 * Field paths are verified against mediawiki.page_change.v1 schema at
 * ~/wmf/gitlab/schemas-event-primary/jsonschema/mediawiki/page/change/latest.yaml
 *
 * Natural key for MERGE:
 *   Revision events: (wiki_id, revision_id) — immutable and unique per wiki.
 *   Page/user events: (wiki_id, event_meta_id) where event_meta_id = meta.id (UUID).
 *     revision_id is NULL for page/user events (wmf.mediawiki_history convention), so
 *     it cannot serve as the key. The timestamp-composite key was ruled out: April 2026
 *     data shows 10 move collisions at second-precision timestamps.
 *
 * Two schema columns support provenance tracking:
 *   event_meta_id STRING: MERGE key for page and user events; NULL for revision rows.
 *   control_map MAP<STRING,STRING>: per-stream rerun guards (one timestamp key per MERGE).
 *     TODO: convert to a typed struct for better schema enforcement.
 *
 * Seven MERGEs — each with a different join key or source, which cannot be unified without
 * violating Iceberg's one-source-row-per-target-row constraint or losing partition pruning:
 *   MERGE 1 (buildMergeSQL):              revision events — keyed on (wiki_id, revision_id).
 *     source='snapshot' rows matched by ON are skipped (no WHEN MATCHED fires), preventing
 *     duplicates when a revision already exists from MWHistorySnapshotMerger.
 *   MERGE 2 (buildBackPatchSQL):          back-patch seed rows whose first reverter arrived today.
 *     Separate MERGE so Iceberg COW can prune by the small back-patch revision_id set.
 *   MERGE 3 (buildTagsMergeSQL):          revision_tags — 90-day target bound for partition pruning.
 *   MERGE 4 (buildVisibilityMergeSQL):    revision_deleted_parts — no timestamp bound (visibility
 *     changes target all-time revisions; 46% are >1 year old).
 *   MERGE 5 (buildPageEventMergeSQL):     page events — keyed on (wiki_id, event_meta_id).
 *   MERGE 6 (buildPageDeletionBackpatchSQL): page deletion back-patch — joins on (wiki_id, page_id),
 *     updates page_is_deleted and revision_is_deleted_by_page_deletion on revision rows.
 *   MERGE 7 (buildUserEventMergeSQL):     user events — keyed on (wiki_id, event_meta_id).
 *     MERGE 7b (buildUserCreationProvenanceBackfillSQL): back-fills user_is_created_by_* on
 *       rename/altergroups rows inserted by MERGE 7.
 */
object MWHistoryDeltaWriter {

  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  case class Params(
    pageChangeTable: String    = "",  // e.g. event.mediawiki_page_change
    targetTable: String        = "",  // mediawiki_history_incremental_v1
    namespacesTable: String    = "",  // wmf_raw.mediawiki_project_namespace_map
    namespacesSnapshot: String = "",  // YYYY-MM snapshot for the namespaces table
    tagsTable: String          = "",  // e.g. event.mediawiki_revision_tags_change
    visibilityTable: String    = "",  // e.g. event.mediawiki_revision_visibility_change
    userChangeTable: String    = "",  // e.g. event.mediawiki_user_change
    year: Int       = 0,
    month: Int      = 0,
    day: Int        = 0,
    catalog: String       = "",  // Spark catalog (e.g. spark_catalog); required when using icebergBranch
    icebergBranch: String = ""   // Iceberg branch name for atomic publishing; empty = write directly to main
  ) {
    def ref: String = IcebergBranchOps.targetRef(catalog, targetTable, icebergBranch)
  }

  // $COVERAGE-OFF$
  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val spark = SparkSession.builder().getOrCreate()
    run(spark, params)
    spark.stop()
  }
  // $COVERAGE-ON$

  def parseArgs(args: Array[String]): Params = {
    import scopt.OptionParser
    val parser = new OptionParser[Params]("MWHistoryDeltaWriter") {
      opt[String]("page_change_table").required().action((v, p) => p.copy(pageChangeTable = v))
      opt[String]("target_table").required().action((v, p) => p.copy(targetTable = v))
      opt[String]("namespaces_table").required().action((v, p) => p.copy(namespacesTable = v))
      opt[String]("namespaces_snapshot").required().action((v, p) => p.copy(namespacesSnapshot = v))
      opt[String]("tags_table").required().action((v, p) => p.copy(tagsTable = v))
      opt[String]("visibility_table").required().action((v, p) => p.copy(visibilityTable = v))
      opt[String]("user_change_table").required().action((v, p) => p.copy(userChangeTable = v))
      opt[Int]("year").required().action((v, p) => p.copy(year = v))
      opt[Int]("month").required().action((v, p) => p.copy(month = v))
      opt[Int]("day").required().action((v, p) => p.copy(day = v))
      opt[String]("catalog").action((v, p) => p.copy(catalog = v))
      opt[String]("iceberg_branch").action((v, p) => p.copy(icebergBranch = v))
    }
    parser.parse(args, Params()).getOrElse(sys.exit(1))
  }


  def run(spark: SparkSession, p: Params): Unit = {
    log.info(s"MWHistoryDeltaWriter params: $p")
    // map_concat with a key already present in the target control_map throws
    // DUPLICATED_MAP_KEY in Spark 3.5's strict mode. LAST_WIN restores the intended
    // semantics: the right-hand map (new value) wins on key conflicts.
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    val baseTable = IcebergBranchOps.targetRef(p.catalog, p.targetTable, "")
    if (p.icebergBranch.nonEmpty) {
      val dropSql   = s"ALTER TABLE $baseTable DROP BRANCH IF EXISTS ${p.icebergBranch}"
      val createSql = s"ALTER TABLE $baseTable CREATE BRANCH ${p.icebergBranch}"
      log.info(s"MWHistoryDeltaWriter WAP: dropping branch if exists:\n$dropSql")
      spark.sql(dropSql)
      log.info(s"MWHistoryDeltaWriter WAP: creating branch:\n$createSql")
      spark.sql(createSql)
    }
    // MERGE 1: insert/update today's revision events.
    val mergeSql = buildMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter MERGE INTO ${p.ref}:\n$mergeSql")
    spark.sql(mergeSql).collect()
    // MERGE 2: back-patch seed rows whose first reverter arrived today.
    val backPatchSql = buildBackPatchSQL(p)
    log.info(s"Running MWHistoryDeltaWriter back-patch MERGE INTO ${p.ref}:\n$backPatchSql")
    spark.sql(backPatchSql).collect()
    // MERGE 3: update revision_tags from today's revision_tags_change events.
    // Separate stream from page_change_v1; tags are applied asynchronously after revision creation.
    val tagsSql = buildTagsMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter tags MERGE INTO ${p.ref}:\n$tagsSql")
    spark.sql(tagsSql).collect()
    // MERGE 4: update revision_deleted_parts from today's revision_visibility_change events.
    // Visibility changes are applied independently of edits; the 90-day lower bound on the
    // target matches the tags window and enables partition pruning.
    val visibilitySql = buildVisibilityMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter visibility MERGE INTO ${p.ref}:\n$visibilitySql")
    spark.sql(visibilitySql).collect()
    // MERGE 5: insert/update today's page events (move, delete, undelete).
    // Keyed on (wiki_id, event_meta_id) where event_meta_id = meta.id. revision_id is NULL
    // for all page events (matches wmf.mediawiki_history convention).
    val pageEventSql = buildPageEventMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter page events MERGE INTO ${p.ref}:\n$pageEventSql")
    spark.sql(pageEventSql).collect()
    // MERGE 6: back-patch page_is_deleted and revision_is_deleted_by_page_deletion on existing
    // revision rows when today's page events include a delete or undelete. Runs after M5 so
    // the page_deletion state from today is committed before the revision back-patch reads it.
    val pageDeletionBackpatchSql = buildPageDeletionBackpatchSQL(p)
    log.info(s"Running MWHistoryDeltaWriter page deletion back-patch MERGE INTO ${p.ref}:\n$pageDeletionBackpatchSql")
    spark.sql(pageDeletionBackpatchSql).collect()
    // MERGE 7: insert/update today's user events (create, rename, groups_change).
    // Keyed on (wiki_id, event_meta_id) where event_meta_id = meta.id. revision_id is NULL
    // for all user events (matches wmf.mediawiki_history convention).
    val userEventSql = buildUserEventMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter user events MERGE INTO ${p.ref}:\n$userEventSql")
    spark.sql(userEventSql).collect()
    // MERGE 7b: back-fill user_is_created_by_* on today's rename/altergroups rows.
    // The stream carries no creation-provenance for non-create events; this joins the
    // newly-inserted rows to the corresponding create-event row already in the target table.
    val userCreationProvenanceSql = buildUserCreationProvenanceBackfillSQL(p)
    log.info(s"Running MWHistoryDeltaWriter user creation provenance back-fill MERGE INTO ${p.ref}:\n$userCreationProvenanceSql")
    spark.sql(userCreationProvenanceSql).collect()
    if (p.icebergBranch.nonEmpty) {
      val ffSql   = s"CALL ${p.catalog}.system.fast_forward('${p.targetTable}', 'main', '${p.icebergBranch}')"
      val dropSql = s"ALTER TABLE $baseTable DROP BRANCH IF EXISTS ${p.icebergBranch}"
      log.info(s"MWHistoryDeltaWriter WAP: fast-forwarding main to branch:\n$ffSql")
      spark.sql(ffSql)
      log.info(s"MWHistoryDeltaWriter WAP: dropping branch:\n$dropSql")
      spark.sql(dropSql)
    }
  }

  /**
   * Returns the CTE chain (WITH ... incoming AS (...)) without the MERGE.
   * Tests append "SELECT * FROM incoming" to exercise the logic without Iceberg.
   */
  def buildIncomingSQL(p: Params): String = {
    val paddedMonth = "%02d".format(p.month)
    val paddedDay   = "%02d".format(p.day)

    s"""WITH raw_events AS (
  -- Map page_change fields to the target schema column names.
  -- Only 'create' and 'edit' produce revision rows; moves/deletes are handled separately.
  --
  -- Field path notes (verified against event.mediawiki_page_change_v1 Hive schema):
  -- (1) is_anonymous: no explicit field — derived as user_id IS NULL.
  -- (2) registration_dt: present in revision.editor.registration_dt.
  -- (3) rev_sha1: use revision.rev_sha1 (all-slots sha1, matches wmf.mediawiki_history).
  -- (4) byte diff: prior_state.revision.rev_size carries parent size — no join needed.
  --     NULL for page creates where prior_state.revision is absent.
  -- (5) bot-by-group: revision.editor.groups is present in the event.
  -- (6) revision_tags: not present in the event schema — written as NULL.
  SELECT
    wiki_id,
    'revision'                                                      AS event_entity,
    page_change_kind                                                AS event_type,
    to_timestamp(revision.rev_dt)                                   AS event_timestamp,
    revision.editor.user_id                                         AS event_user_id,
    revision.editor.user_central_id                                 AS event_user_central_id,
    revision.editor.user_text                                       AS event_user_text_historical,
    (revision.editor.user_id IS NULL)                               AS event_user_is_anonymous,
    revision.editor.is_temp                                         AS event_user_is_temporary,
    (revision.editor.user_id IS NOT NULL
     AND NOT revision.editor.is_temp)                               AS event_user_is_permanent,
    to_timestamp(revision.editor.registration_dt)                   AS event_user_registration_timestamp,
    revision.editor.edit_count                                      AS event_user_revision_count,
    page.page_id                                                    AS page_id,
    page.page_title                                                 AS page_title_historical,
    page.namespace_id                                               AS page_namespace_historical,
    revision.rev_id                                                 AS revision_id,
    revision.rev_parent_id                                          AS revision_parent_id,
    revision.is_minor_edit                                          AS revision_minor_edit,
    CAST(revision.rev_size AS BIGINT)                               AS revision_text_bytes,
    CAST(revision.rev_size AS BIGINT)
      - CAST(prior_state.revision.rev_size AS BIGINT)               AS revision_text_bytes_diff,
    revision.rev_sha1                                               AS revision_text_sha1,
    CAST(NULL AS ARRAY<STRING>)                                     AS revision_tags,
    CASE WHEN NOT (revision.is_content_visible
                   AND revision.is_editor_visible
                   AND revision.is_comment_visible)
         THEN filter(
                array(
                  CASE WHEN NOT revision.is_content_visible THEN 'text'    END,
                  CASE WHEN NOT revision.is_comment_visible THEN 'comment' END,
                  CASE WHEN NOT revision.is_editor_visible  THEN 'user'    END
                ),
                x -> x IS NOT NULL
              )
    END                                                             AS revision_deleted_parts,
    revision.editor.groups                                          AS user_groups_raw,
    to_timestamp(meta.dt)                                           AS meta_dt
  FROM ${p.pageChangeTable}
  WHERE year  = ${p.year}
    AND month = ${p.month}
    AND day   = ${p.day}
    AND page_change_kind IN ('create', 'edit')
    -- Exclude page imports and other admin operations that emit 'create'/'edit' events
    -- for revisions with old rev_dt. Normal edits always have rev_dt ≈ ingestion dt.
    -- 90-day bound matches the revert and tags windows.
    AND to_timestamp(revision.rev_dt) >= TIMESTAMP '${p.year}-${paddedMonth}-${paddedDay} 00:00:00' - INTERVAL 90 DAYS
),

deduplicated AS (
  -- Keep the latest event per (wiki_id, revision_id) when duplicates arrive.
  SELECT * FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY wiki_id, revision_id
        ORDER BY meta_dt DESC
      ) AS rn
    FROM raw_events
  )
  WHERE rn = 1
),

with_namespace AS (
  -- INNER JOIN drops events whose (wiki_id, namespace_id) is not in the map snapshot
  -- (wiki not yet onboarded, or unusual namespace IDs). Better to skip than to emit
  -- a row with NULL page_namespace_is_content_historical.
  SELECT
    e.*,
    (ns.namespace_is_content = 1) AS page_namespace_is_content_historical
  FROM deduplicated e
  JOIN ${p.namespacesTable} ns
    ON  e.wiki_id                   = ns.dbname
    AND e.page_namespace_historical = ns.namespace
    AND ns.snapshot                 = '${p.namespacesSnapshot}'
),

with_bots AS (
  -- Bot-by-name: same regex as UserEventBuilder.botUsernamePattern.
  -- Bot-by-group: revision.editor.groups is present in page_change_v1.
  SELECT
    e.*,
    filter(
      array(
        CASE WHEN lower(e.event_user_text_historical) RLIKE '(?i)^.*bot([^a-z].*$$|$$)'
             THEN 'name' END,
        CASE WHEN array_contains(e.user_groups_raw, 'bot')
             THEN 'group' END
      ),
      x -> x IS NOT NULL
    ) AS event_user_is_bot_by_historical
  FROM with_namespace e
),

revert_seed AS (
  -- Target rows on pages touched by today's incoming batch. Page-scoped (not sha1-scoped)
  -- because a wider revert can be caused by a reverter whose base shares no sha1 with
  -- anything in today's incoming.
  --
  -- Filters:
  --   revision_text_sha1 IS NOT NULL — the algorithm's ranking is meaningful only for
  --     rows with a hashed payload; rows with NULL sha1 would partition together
  --     (Spark treats NULL = NULL in PARTITION BY) and produce spurious sha-series.
  --   ANTI JOIN deduplicated — keeps re-streamed today's revisions out of the seed so
  --     a row cannot be both seed and incoming (which would otherwise self-revert).
  --
  -- existing_revision_is_identity_reverted and revision_parent_id are forwarded so the
  -- candidates filter can apply the annotation-aware skip and the parent-id page-merge
  -- rule in the same pass.
  SELECT
    t.wiki_id,
    t.page_id,
    t.revision_id,
    t.revision_text_sha1,
    t.revision_parent_id,
    t.event_timestamp,
    t.revision_is_identity_reverted   AS existing_revision_is_identity_reverted,
    FALSE                             AS is_incoming
  FROM ${p.ref} t
  LEFT ANTI JOIN deduplicated d ON t.wiki_id = d.wiki_id AND t.revision_id = d.revision_id
  WHERE t.page_id IN (SELECT page_id FROM deduplicated)
    AND t.revision_text_sha1 IS NOT NULL
),

revert_candidates AS (
  -- Today's incoming revisions in the same shape as revert_seed, plus the seed rows.
  SELECT
    wiki_id, page_id, revision_id, revision_text_sha1, revision_parent_id, event_timestamp,
    CAST(NULL AS BOOLEAN) AS existing_revision_is_identity_reverted,
    TRUE                  AS is_incoming
  FROM deduplicated
  WHERE revision_text_sha1 IS NOT NULL
  UNION ALL
  SELECT * FROM revert_seed
),

sha1_ranked AS (
  -- Rank occurrences of each (wiki_id, page_id, sha1) by (event_timestamp, revision_id),
  -- identify the rank=1 row as the sha-series base (own_base_*), and classify each rank>=2
  -- row as a real reverter or a no-op duplicate.
  --
  -- A rank>=2 row whose revision_parent_id equals the IMMEDIATELY PRECEDING same-sha row
  -- on the same page is a NO-OP — its content already matches its parent's content, so it
  -- isn't restoring anything earlier. Examples: page-merge artifacts, null edits,
  -- replay-emitted duplicates. Aligned with Gerrit 1295404 (monthly canonical fix).
  --
  -- The LAG(revision_id) over the same partition spec catches the "parent is the previous
  -- same-sha rev" case. The monthly implements this via a foldLeft; LAG diverges only on
  -- chains of >=3 consecutive no-ops (vanishingly rare in real data).
  --
  -- The four window functions share one PARTITION BY + ORDER BY spec — Spark fuses them
  -- into a single WindowExec, so LAG adds no shuffle, no sort, just one extra pass over
  -- already-sorted partition rows.
  SELECT *,
    (sha1_rank > 1 AND revision_parent_id IS DISTINCT FROM prev_in_sha_series_rev_id) AS is_real_reverter
  FROM (
    SELECT
      wiki_id, page_id, revision_id, revision_text_sha1, revision_parent_id, event_timestamp,
      is_incoming, existing_revision_is_identity_reverted,
      row_number() OVER (
        PARTITION BY wiki_id, page_id, revision_text_sha1
        ORDER BY event_timestamp, revision_id
      ) AS sha1_rank,
      lag(revision_id) OVER (
        PARTITION BY wiki_id, page_id, revision_text_sha1
        ORDER BY event_timestamp, revision_id
      ) AS prev_in_sha_series_rev_id,
      first_value(revision_id) OVER (
        PARTITION BY wiki_id, page_id, revision_text_sha1
        ORDER BY event_timestamp, revision_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS own_base_rev_id,
      first_value(event_timestamp) OVER (
        PARTITION BY wiki_id, page_id, revision_text_sha1
        ORDER BY event_timestamp, revision_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS own_base_ts
    FROM revert_candidates
  )
),

reverters_with_base AS (
  -- The small broadcast set: incoming reverters that are REAL (not no-op duplicates).
  -- Seed reverters can never be the answer for either MERGE under the in-order daily
  -- pipeline invariant (seed.max_ts < incoming.min_ts), so filtering them out pre-broadcast
  -- is correct AND a key perf win. The no-op filter via `is_real_reverter` further shrinks
  -- the broadcast set and propagates the rule uniformly to both M1 and M2.
  --
  -- The filtered set is ~2 MB/day at WMF scale (~23k rows on the busiest May 2026 day,
  -- ~80B per row), well under Spark's default 10 MB autoBroadcastJoinThreshold.
  SELECT
    wiki_id, page_id,
    revision_id        AS reverter_rev_id,
    event_timestamp    AS reverter_ts,
    own_base_rev_id    AS reverter_base_id,
    own_base_ts        AS reverter_base_ts
  FROM sha1_ranked
  WHERE is_real_reverter = TRUE
    AND is_incoming = TRUE
),

-- ============================================================================
-- candidates / first_reverter are SPLIT per-MERGE so each MERGE's `candidates`
-- filter is tightly scoped to the rows it actually needs. A unified candidates
-- CTE (with `is_incoming = TRUE OR existing IS NOT TRUE` admitting both rev
-- sides) was tried and reverted: each MERGE re-runs the chain on the union,
-- doubling the upstream work for either MERGE. The wider-revert per-MERGE
-- filters cut the join input by orders of magnitude on hot pages.
--
-- Both candidates CTEs share the same wider-revert window predicate:
--   (B.ts, B.rev_id) < (R.ts, R.rev_id)  -- base was processed strictly before R
--   (r.ts, r.rev_id) > (R.ts, R.rev_id)  -- r is not yet processed; not R itself
-- These ties-by-id predicates mirror the TreeSet[(ts, id), baseId] ordering used
-- by DenormalizedRevisionsBuilder.MutableOrderedReverts. If R is itself a
-- sha-series base (sha1_rank = 1), the strict-base-less predicate excludes R's
-- own reverters — matching the monthly's "add reverters AFTER processing the
-- base" iteration order.
--
-- Correctness assumes in-order daily runs: seed.max_ts < incoming.min_ts.
-- Out-of-order backfills or late-arriving events leave a bounded gap healed
-- by MWHistorySnapshotMerger at monthly close.
-- ============================================================================

candidates_m1 AS (
  -- MERGE 1: annotate today's incoming revisions. rev is restricted to incoming
  -- so MERGE 1 only computes first_reverter for rows it will actually update/insert.
  SELECT /*+ BROADCAST(r) */
    rev.wiki_id, rev.page_id, rev.revision_id, rev.event_timestamp AS rev_ts,
    rev.sha1_rank, rev.own_base_rev_id, rev.is_real_reverter,
    r.reverter_rev_id, r.reverter_ts, r.reverter_base_id
  FROM sha1_ranked rev
  JOIN reverters_with_base r
    ON rev.wiki_id = r.wiki_id AND rev.page_id = r.page_id
  WHERE rev.is_incoming = TRUE
    AND r.reverter_rev_id != rev.revision_id
    AND (r.reverter_base_ts <  rev.event_timestamp
         OR (r.reverter_base_ts = rev.event_timestamp AND r.reverter_base_id < rev.revision_id))
    AND (r.reverter_ts >  rev.event_timestamp
         OR (r.reverter_ts = rev.event_timestamp AND r.reverter_rev_id > rev.revision_id))
),

candidates_m2 AS (
  -- MERGE 2: back-patch seed revisions whose first pending reverter is in today's
  -- batch. rev is restricted to seed rows that are not yet annotated:
  --   - rev.is_incoming = FALSE: only seed rows are candidates for back-patch.
  --   - existing_revision_is_identity_reverted IS NOT TRUE: already-annotated seed
  --     rows can't be improved by today's reverters (in-order pipeline → today's
  --     reverter has a later ts than the one that wrote the existing annotation,
  --     so MIN-by-(ts, id) would keep the existing answer). Skip pre-join.
  SELECT /*+ BROADCAST(r) */
    rev.wiki_id, rev.page_id, rev.revision_id, rev.event_timestamp AS rev_ts,
    rev.sha1_rank, rev.own_base_rev_id, rev.is_real_reverter,
    r.reverter_rev_id, r.reverter_ts, r.reverter_base_id
  FROM sha1_ranked rev
  JOIN reverters_with_base r
    ON rev.wiki_id = r.wiki_id AND rev.page_id = r.page_id
  WHERE rev.is_incoming = FALSE
    AND rev.existing_revision_is_identity_reverted IS NOT TRUE
    AND r.reverter_rev_id != rev.revision_id
    AND (r.reverter_base_ts <  rev.event_timestamp
         OR (r.reverter_base_ts = rev.event_timestamp AND r.reverter_base_id < rev.revision_id))
    AND (r.reverter_ts >  rev.event_timestamp
         OR (r.reverter_ts = rev.event_timestamp AND r.reverter_rev_id > rev.revision_id))
),

first_reverter_m1 AS (
  -- Two reverter picks per rev — global MIN and diff-base MIN:
  --   - Non-reverter R (base or no-op): reverted by any pending reverter → global MIN.
  --   - Real-reverter R: reverted only by a reverter with a DIFFERENT base (the "wider
  --     revert" branch) → diff-base MIN. Using the global MIN here is wrong on inner-
  --     revert chains where same-base reverters precede the first different-base one.
  SELECT
    wiki_id, page_id, revision_id,
    min_by(reverter_rev_id,  struct(reverter_ts, reverter_rev_id)) AS fr_global_rev_id,
    min_by(reverter_ts,      struct(reverter_ts, reverter_rev_id)) AS fr_global_ts,
    min_by(reverter_rev_id,  struct(reverter_ts, reverter_rev_id))
      FILTER (WHERE reverter_base_id != own_base_rev_id)            AS fr_diff_rev_id,
    min_by(reverter_ts,      struct(reverter_ts, reverter_rev_id))
      FILTER (WHERE reverter_base_id != own_base_rev_id)            AS fr_diff_ts
  FROM candidates_m1
  GROUP BY wiki_id, page_id, revision_id
),

first_reverter_m2 AS (
  -- Same aggregation as first_reverter_m1; keys forwarded so back_patch can pick
  -- the effective fr based on is_real_reverter without re-joining sha1_ranked.
  SELECT
    wiki_id, page_id, revision_id, own_base_rev_id, is_real_reverter, rev_ts,
    min_by(reverter_rev_id,  struct(reverter_ts, reverter_rev_id)) AS fr_global_rev_id,
    min_by(reverter_ts,      struct(reverter_ts, reverter_rev_id)) AS fr_global_ts,
    min_by(reverter_rev_id,  struct(reverter_ts, reverter_rev_id))
      FILTER (WHERE reverter_base_id != own_base_rev_id)            AS fr_diff_rev_id,
    min_by(reverter_ts,      struct(reverter_ts, reverter_rev_id))
      FILTER (WHERE reverter_base_id != own_base_rev_id)            AS fr_diff_ts
  FROM candidates_m2
  GROUP BY wiki_id, page_id, revision_id, own_base_rev_id, is_real_reverter, rev_ts
),

revert_annotations AS (
  -- Annotations for INCOMING revisions. Drives MERGE 1's INSERT/UPDATE branch.
  --   is_revert   = s.is_real_reverter — TRUE iff sha1_rank > 1 AND NOT a no-op duplicate.
  --   is_reverted = effective first reverter exists, where "effective" is:
  --                   - non-reverter R (base or no-op): the global MIN pending reverter,
  --                   - real-reverter R: the MIN pending reverter with a DIFFERENT base
  --                     from R's own (the "different wider revert" branch — matches
  --                     DenormalizedRevisionsBuilder updateRevisionAndReverts after the
  --                     Gerrit 1295404 fix to handle inner-revert chains correctly).
  SELECT
    wiki_id,
    revision_id,
    CASE WHEN is_real_reverter THEN TRUE END                              AS revision_is_identity_revert,
    CASE WHEN fr_eff_rev_id IS NULL THEN CAST(NULL AS BOOLEAN) ELSE TRUE END AS revision_is_identity_reverted,
    fr_eff_rev_id                                                         AS revision_first_identity_reverting_revision_id,
    CASE
      WHEN fr_eff_rev_id IS NULL THEN CAST(NULL AS BIGINT)
      ELSE CAST(unix_timestamp(fr_eff_ts) - unix_timestamp(rev_ts) AS BIGINT)
    END                                                                   AS revision_seconds_to_identity_revert
  FROM (
    SELECT
      s.wiki_id, s.revision_id, s.event_timestamp AS rev_ts, s.is_real_reverter,
      CASE WHEN s.is_real_reverter THEN fr.fr_diff_rev_id ELSE fr.fr_global_rev_id END AS fr_eff_rev_id,
      CASE WHEN s.is_real_reverter THEN fr.fr_diff_ts     ELSE fr.fr_global_ts     END AS fr_eff_ts
    FROM sha1_ranked s
    LEFT JOIN first_reverter_m1 fr
      ON  s.wiki_id     = fr.wiki_id
      AND s.revision_id = fr.revision_id
    WHERE s.is_incoming
  )
),

back_patch AS (
  -- Updates for SEED revisions whose effective first pending reverter is in today's
  -- incoming. Drives MERGE 2. candidates_m2 already filtered rev to seed-only, so every
  -- row in first_reverter_m2 is a seed candidate. All reverters in reverters_with_base
  -- are real incoming reverters by construction (is_real_reverter AND is_incoming).
  --
  -- Effective fr is picked the same way as revert_annotations: global MIN for non-(real-)
  -- reverters, diff-base MIN for real reverters. Emit when the effective fr exists.
  --
  -- The reverter's meta_dt is read from with_bots so the control_map's revert_patch_dt
  -- key reflects the delivery time of the row that caused the back-patch.
  SELECT
    fr.wiki_id,
    fr.revision_id                                                       AS target_revision_id,
    fr.fr_eff_rev_id                                                     AS first_reverting_rev_id,
    CAST(unix_timestamp(fr.fr_eff_ts) - unix_timestamp(fr.rev_ts) AS BIGINT) AS seconds_to_revert,
    e.meta_dt                                                            AS reverter_meta_dt
  FROM (
    SELECT
      f.wiki_id, f.revision_id, f.rev_ts,
      CASE WHEN f.is_real_reverter THEN f.fr_diff_rev_id ELSE f.fr_global_rev_id END AS fr_eff_rev_id,
      CASE WHEN f.is_real_reverter THEN f.fr_diff_ts     ELSE f.fr_global_ts     END AS fr_eff_ts
    FROM first_reverter_m2 f
  ) fr
  JOIN with_bots e
    ON fr.wiki_id = e.wiki_id AND fr.fr_eff_rev_id = e.revision_id
  WHERE fr.fr_eff_rev_id IS NOT NULL
),

incoming AS (
  -- Full incoming rows. MERGE 1 WHEN MATCHED uses these to UPDATE all event/revert
  -- fields; WHEN NOT MATCHED uses them to INSERT a new row.
  SELECT
    'events'                                                              AS source,
    e.wiki_id,
    e.event_entity,
    e.event_type,
    e.event_timestamp,
    e.event_user_id,
    e.event_user_central_id,
    e.event_user_text_historical,
    e.event_user_is_anonymous,
    e.event_user_is_temporary,
    e.event_user_is_permanent,
    e.event_user_registration_timestamp,

    e.page_id,
    e.page_title_historical,
    e.page_namespace_historical,
    e.revision_id,
    e.revision_parent_id,
    e.revision_minor_edit,
    e.revision_text_bytes,
    e.revision_text_bytes_diff,
    e.revision_text_sha1,
    e.revision_tags,
    e.revision_deleted_parts,
    e.page_namespace_is_content_historical,
    e.event_user_is_bot_by_historical,
    e.event_user_revision_count,
    -- Fresh revisions (rev_dt = today) cannot have been reverted yet; FALSE is safe and
    -- M2 will update to TRUE if a reverter arrives later. Late revisions (rev_dt < today)
    -- keep NULL: a reverter may already be in the target from a prior day and M2 won't
    -- catch it; the monthly snapshot merge provides the correct value.
    -- NULL-sha1 rows are excluded from revert detection entirely; keep NULL for them.
    CASE WHEN ra.revision_is_identity_reverted IS TRUE THEN TRUE
         WHEN e.event_timestamp >= TIMESTAMP '${p.year}-${paddedMonth}-${paddedDay} 00:00:00'
          AND e.revision_text_sha1 IS NOT NULL THEN FALSE
         ELSE NULL
    END                                                               AS revision_is_identity_reverted,
    ra.revision_first_identity_reverting_revision_id,
    ra.revision_seconds_to_identity_revert,
    CASE WHEN ra.revision_is_identity_revert IS TRUE THEN TRUE
         WHEN e.event_timestamp >= TIMESTAMP '${p.year}-${paddedMonth}-${paddedDay} 00:00:00'
          AND e.revision_text_sha1 IS NOT NULL THEN FALSE
         ELSE NULL
    END                                                               AS revision_is_identity_revert,
    (e.event_user_text_historical LIKE '%>%'
     AND e.event_user_is_anonymous
     AND NOT COALESCE(e.event_user_is_temporary, FALSE))              AS event_user_is_cross_wiki,
    FALSE                                                              AS page_is_deleted,
    FALSE                                                              AS revision_is_deleted_by_page_deletion,
    CAST(NULL AS BIGINT)                                               AS user_central_id,
    e.meta_dt
  FROM with_bots e
  LEFT JOIN revert_annotations ra
    ON  e.wiki_id     = ra.wiki_id
    AND e.revision_id = ra.revision_id
)"""
  }

  def buildMergeSQL(p: Params): String =
    buildIncomingSQL(p) +
    s"""

MERGE INTO ${p.ref} t
USING incoming s
ON  t.wiki_id     = s.wiki_id
AND t.revision_id = s.revision_id
WHEN MATCHED AND t.source = 'events' THEN
  UPDATE SET
    t.event_type                                                    = s.event_type,
    t.event_timestamp                                               = s.event_timestamp,
    t.event_user_id                                                 = s.event_user_id,
    t.event_user_central_id                                         = s.event_user_central_id,
    t.event_user_text_historical                                    = s.event_user_text_historical,
    t.event_user_is_anonymous                                       = s.event_user_is_anonymous,
    t.event_user_is_temporary                                       = s.event_user_is_temporary,
    t.event_user_is_permanent                                       = s.event_user_is_permanent,
    t.event_user_registration_timestamp                             = s.event_user_registration_timestamp,
    t.page_id                                                       = s.page_id,
    t.page_title_historical                                         = s.page_title_historical,
    t.page_namespace_historical                                     = s.page_namespace_historical,
    t.revision_parent_id                                            = s.revision_parent_id,
    t.revision_minor_edit                                           = s.revision_minor_edit,
    t.revision_text_bytes                                           = s.revision_text_bytes,
    t.revision_text_bytes_diff                                      = s.revision_text_bytes_diff,
    t.revision_text_sha1                                            = s.revision_text_sha1,
    t.revision_tags                                                 = s.revision_tags,
    t.revision_deleted_parts                                        = s.revision_deleted_parts,
    t.page_namespace_is_content_historical                          = s.page_namespace_is_content_historical,
    t.event_user_is_bot_by_historical                               = s.event_user_is_bot_by_historical,
    t.revision_is_identity_reverted                                 = s.revision_is_identity_reverted,
    t.revision_first_identity_reverting_revision_id                 = s.revision_first_identity_reverting_revision_id,
    t.revision_seconds_to_identity_revert                           = s.revision_seconds_to_identity_revert,
    t.revision_is_identity_revert                                   = s.revision_is_identity_revert,
    t.event_user_is_cross_wiki                                      = s.event_user_is_cross_wiki,
    t.page_is_deleted                                               = s.page_is_deleted,
    t.revision_is_deleted_by_page_deletion                          = s.revision_is_deleted_by_page_deletion,
    t.control_map = map_concat(COALESCE(t.control_map, map()), map('revision_update_dt', CAST(s.meta_dt AS STRING)))
WHEN NOT MATCHED THEN
  INSERT (
    source,
    wiki_id,
    event_meta_id,
    event_entity,
    event_type,
    event_timestamp,
    event_user_id,
    event_user_central_id,
    event_user_text_historical,
    event_user_is_anonymous,
    event_user_is_temporary,
    event_user_is_permanent,
    event_user_registration_timestamp,

    page_id,
    page_title_historical,
    page_namespace_historical,
    revision_id,
    revision_parent_id,
    revision_minor_edit,
    revision_text_bytes,
    revision_text_bytes_diff,
    revision_text_sha1,
    revision_tags,
    revision_deleted_parts,
    page_namespace_is_content_historical,
    event_user_is_bot_by_historical,
    event_user_revision_count,
    event_user_groups_historical,
    user_id,
    user_text_historical,
    user_is_anonymous,
    user_is_temporary,
    user_is_permanent,
    user_groups_historical,
    user_is_bot_by_historical,
    user_is_created_by_self,
    user_is_created_by_system,
    user_is_created_by_peer,
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    event_user_is_cross_wiki,
    page_is_deleted,
    revision_is_deleted_by_page_deletion,
    user_central_id,
    control_map
  ) VALUES (
    s.source,
    s.wiki_id,
    CAST(NULL AS STRING),
    s.event_entity,
    s.event_type,
    s.event_timestamp,
    s.event_user_id,
    s.event_user_central_id,
    s.event_user_text_historical,
    s.event_user_is_anonymous,
    s.event_user_is_temporary,
    s.event_user_is_permanent,
    s.event_user_registration_timestamp,
    s.page_id,
    s.page_title_historical,
    s.page_namespace_historical,
    s.revision_id,
    s.revision_parent_id,
    s.revision_minor_edit,
    s.revision_text_bytes,
    s.revision_text_bytes_diff,
    s.revision_text_sha1,
    s.revision_tags,
    s.revision_deleted_parts,
    s.page_namespace_is_content_historical,
    s.event_user_is_bot_by_historical,
    s.event_user_revision_count,
    CAST(NULL AS ARRAY<STRING>),
    CAST(NULL AS BIGINT),
    CAST(NULL AS STRING),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS ARRAY<STRING>),
    CAST(NULL AS ARRAY<STRING>),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    s.event_user_is_cross_wiki,
    s.page_is_deleted,
    s.revision_is_deleted_by_page_deletion,
    s.user_central_id,
    map('revision_update_dt', CAST(s.meta_dt AS STRING))
  )"""

  /**
   * MERGE 2: back-patches seed revisions whose first wider-revert reverter is in today's
   * incoming. Sources from the same buildIncomingSQL CTE chain — the `back_patch` CTE is
   * defined there alongside `incoming`. Two separate MERGE INTOs (M1 + M2) instead of one
   * unified MERGE because Iceberg COW depends on tight target partition pruning per MERGE,
   * which the unified source (with back-patch revision_ids spanning all months) defeats.
   */
  def buildBackPatchSQL(p: Params): String =
    buildIncomingSQL(p) +
    s"""

MERGE INTO ${p.ref} t
USING back_patch s
ON  t.wiki_id     = s.wiki_id
AND t.revision_id = s.target_revision_id
WHEN MATCHED THEN
  UPDATE SET
    t.revision_is_identity_reverted                                = TRUE,
    t.revision_first_identity_reverting_revision_id                = s.first_reverting_rev_id,
    t.revision_seconds_to_identity_revert                          = s.seconds_to_revert,
    t.control_map = map_concat(COALESCE(t.control_map, map()), map('revert_patch_dt', CAST(s.reverter_meta_dt AS STRING)))"""

  /**
   * Returns the CTE that deduplicates today's revision_tags_change events to one row per revision.
   * Tests append "SELECT * FROM latest_tags" to verify the logic without Iceberg.
   */
  def buildTagsCteSQL(p: Params): String =
    s"""WITH latest_tags AS (
  -- Most recent tag state per revision for the day.
  -- Tags can change multiple times in a day; the last event (by meta.dt) wins.
  SELECT
    database AS wiki_id,
    rev_id   AS revision_id,
    tags     AS revision_tags,
    meta.dt  AS meta_dt
  FROM (
    SELECT *,
      row_number() OVER (
        PARTITION BY database, rev_id
        ORDER BY meta.dt DESC
      ) AS rn
    FROM ${p.tagsTable}
    WHERE year  = ${p.year}
      AND month = ${p.month}
      AND day   = ${p.day}
  )
  WHERE rn = 1
)"""

  def buildTagsMergeSQL(p: Params): String = {
    val paddedMonth = "%02d".format(p.month)
    val paddedDay   = "%02d".format(p.day)

    buildTagsCteSQL(p) +
    s"""

MERGE INTO ${p.ref} t
USING latest_tags s
ON  t.wiki_id         = s.wiki_id
AND t.revision_id     = s.revision_id
AND t.event_timestamp >= TIMESTAMP '${p.year}-${paddedMonth}-${paddedDay} 00:00:00' - INTERVAL 90 DAYS
WHEN MATCHED THEN
  UPDATE SET
    t.revision_tags = s.revision_tags,
    t.control_map   = map_concat(COALESCE(t.control_map, map()), map('tags_update_dt', s.meta_dt))"""
  }

  /**
   * Returns the CTE that deduplicates today's revision_visibility_change events to one row per revision.
   * Tests append "SELECT * FROM latest_visibility" to verify the logic without Iceberg.
   */
  def buildVisibilityCteSQL(p: Params): String =
    s"""WITH latest_visibility AS (
  -- Most recent visibility state per revision for the day.
  -- visibility.text=false → 'text'; .comment=false → 'comment'; .user=false → 'user'.
  SELECT
    database AS wiki_id,
    rev_id   AS revision_id,
    CASE WHEN NOT (visibility.text AND visibility.user AND visibility.comment)
         THEN filter(
                array(
                  CASE WHEN NOT visibility.text    THEN 'text'    END,
                  CASE WHEN NOT visibility.comment THEN 'comment' END,
                  CASE WHEN NOT visibility.user    THEN 'user'    END
                ),
                x -> x IS NOT NULL
              )
    END AS revision_deleted_parts,
    meta.dt AS meta_dt
  FROM (
    SELECT *,
      row_number() OVER (
        PARTITION BY database, rev_id
        ORDER BY meta.dt DESC
      ) AS rn
    FROM ${p.visibilityTable}
    WHERE year  = ${p.year}
      AND month = ${p.month}
      AND day   = ${p.day}
  )
  WHERE rn = 1
)"""

  def buildVisibilityMergeSQL(p: Params): String =
    buildVisibilityCteSQL(p) +
    s"""

-- latest_visibility is ~50 bytes/row; p95 daily volume is ~2,700 rows (~130 KB),
-- well under Spark's 10 MB autoBroadcastJoinThreshold — Spark broadcasts it automatically.
MERGE INTO ${p.ref} t
USING latest_visibility s
ON  t.wiki_id     = s.wiki_id
AND t.revision_id = s.revision_id
WHEN MATCHED THEN
  UPDATE SET
    t.revision_deleted_parts = s.revision_deleted_parts,
    t.control_map = map_concat(COALESCE(t.control_map, map()), map('visibility_update_dt', s.meta_dt))"""

  /**
   * Returns the CTE chain for page events (move, delete, undelete) from page_change_v1.
   * Tests append "SELECT * FROM page_incoming" to exercise the logic without Iceberg.
   *
   * Field source notes (verified against page_change_v1 schema):
   * (1) dt: top-level event timestamp (action time); revision.rev_dt is the revision's creation time.
   * (2) performer: actor for page admin events; revision.editor is absent for non-edit events.
   * (3) page.page_title is used for all event kinds (post-move destination for moves;
   *     current title for delete/undelete). prior_state.page.page_title (pre-move title) is
   *     intentionally unused: MWH convention stores the post-move title in page_title_historical
   *     (verified by sampling wmf.mediawiki_history: "Half-life_(physics)", not "Half-life").
   *     page.page_title includes the namespace prefix (e.g. "User:Foo") for non-main-namespace
   *     pages; REGEXP_REPLACE strips the prefix when namespace_id != 0, matching the bare-title
   *     convention of wmf.mediawiki_history (namespace stored separately in page_namespace_historical).
   * (4) undelete mapped to 'restore' to match wmf.mediawiki_history vocabulary.
   * (5) revision_id is NULL — page events have no revision in wmf.mediawiki_history.
   * (6) All revert tier fields are NULL — not applicable to page-level events.
   *     Bounded tier is also NULL (not FALSE) unlike revision events.
   * (7) visibility_change kind is excluded: those reach the target via MERGE 4's dedicated stream.
   */
  def buildPageIncomingSQL(p: Params): String = {
    s"""WITH raw_page_events AS (
  SELECT
    wiki_id,
    meta.id                                                             AS meta_id,
    to_timestamp(meta.dt)                                               AS meta_dt,
    'page'                                                              AS event_entity,
    CASE WHEN page_change_kind = 'undelete' THEN 'restore'
         ELSE page_change_kind END                                      AS event_type,
    to_timestamp(dt)                                                    AS event_timestamp,
    performer.user_id                                                   AS event_user_id,
    performer.user_central_id                                           AS event_user_central_id,
    performer.user_text                                                 AS event_user_text_historical,
    (performer.user_id IS NULL)                                         AS event_user_is_anonymous,
    performer.is_temp                                                   AS event_user_is_temporary,
    (performer.user_id IS NOT NULL
     AND NOT performer.is_temp)                                         AS event_user_is_permanent,
    to_timestamp(performer.registration_dt)                             AS event_user_registration_timestamp,

    page.page_id                                                        AS page_id,
    CASE WHEN page.namespace_id = 0 THEN page.page_title
         ELSE REGEXP_REPLACE(page.page_title, '^[^:]+:', '')
    END                                                                  AS page_title_historical,
    page.namespace_id                                                    AS page_namespace_historical,
    CAST(NULL AS BIGINT)                                                AS revision_id,
    CAST(NULL AS BIGINT)                                                AS revision_parent_id,
    CAST(NULL AS BOOLEAN)                                               AS revision_minor_edit,
    CAST(NULL AS BIGINT)                                                AS revision_text_bytes,
    CAST(NULL AS BIGINT)                                                AS revision_text_bytes_diff,
    CAST(NULL AS STRING)                                                AS revision_text_sha1,
    CAST(NULL AS ARRAY<STRING>)                                         AS revision_tags,
    CAST(NULL AS ARRAY<STRING>)                                         AS revision_deleted_parts,
    CASE WHEN page_change_kind = 'delete' THEN TRUE ELSE FALSE END      AS page_is_deleted,
    performer.groups                                                    AS user_groups_raw
  FROM ${p.pageChangeTable}
  WHERE year  = ${p.year}
    AND month = ${p.month}
    AND day   = ${p.day}
    AND page_change_kind IN ('move', 'delete', 'undelete')
),

deduplicated_page AS (
  -- Deduplicate on meta.id (the MERGE 5 join key). Empirically meta.id has zero repeats
  -- in a full month of events (April 2026); this guard handles partition re-reads on reruns.
  -- The composite key (wiki_id, page_id, event_type, event_timestamp) is NOT used for dedup:
  -- move events show 10 second-precision collisions in April 2026 data alone.
  SELECT * FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY wiki_id, meta_id
        ORDER BY meta_dt DESC
      ) AS rn
    FROM raw_page_events
  )
  WHERE rn = 1
),

page_with_namespace AS (
  -- INNER JOIN: drop events with no namespace map hit (see with_namespace in buildIncomingSQL).
  SELECT
    e.*,
    (ns.namespace_is_content = 1) AS page_namespace_is_content_historical
  FROM deduplicated_page e
  JOIN ${p.namespacesTable} ns
    ON  e.wiki_id                   = ns.dbname
    AND e.page_namespace_historical = ns.namespace
    AND ns.snapshot                 = '${p.namespacesSnapshot}'
),

page_with_bots AS (
  SELECT
    e.*,
    filter(
      array(
        CASE WHEN lower(e.event_user_text_historical) RLIKE '(?i)^.*bot([^a-z].*$$|$$)'
             THEN 'name' END,
        CASE WHEN array_contains(e.user_groups_raw, 'bot')
             THEN 'group' END
      ),
      x -> x IS NOT NULL
    ) AS event_user_is_bot_by_historical
  FROM page_with_namespace e
),

page_incoming AS (
  SELECT
    'events'                                                            AS source,
    e.wiki_id,
    e.meta_id                                                           AS event_meta_id,
    e.event_entity,
    e.event_type,
    e.event_timestamp,
    e.event_user_id,
    e.event_user_central_id,
    e.event_user_text_historical,
    e.event_user_is_anonymous,
    e.event_user_is_temporary,
    e.event_user_is_permanent,
    e.event_user_registration_timestamp,

    e.page_id,
    e.page_title_historical,
    e.page_namespace_historical,
    e.revision_id,
    e.revision_parent_id,
    e.revision_minor_edit,
    e.revision_text_bytes,
    e.revision_text_bytes_diff,
    e.revision_text_sha1,
    e.revision_tags,
    e.revision_deleted_parts,
    e.page_namespace_is_content_historical,
    e.event_user_is_bot_by_historical,
    CAST(NULL AS BIGINT)                                                AS event_user_revision_count,
    CAST(NULL AS BOOLEAN)                                               AS revision_is_identity_reverted,
    CAST(NULL AS BIGINT)                                                AS revision_first_identity_reverting_revision_id,
    CAST(NULL AS BIGINT)                                                AS revision_seconds_to_identity_revert,
    CAST(NULL AS BOOLEAN)                                               AS revision_is_identity_revert,
    (e.event_user_text_historical LIKE '%>%'
     AND e.event_user_is_anonymous
     AND NOT COALESCE(e.event_user_is_temporary, FALSE))               AS event_user_is_cross_wiki,
    e.page_is_deleted,
    CAST(NULL AS BOOLEAN)                                              AS revision_is_deleted_by_page_deletion,
    CAST(NULL AS BIGINT)                                               AS user_central_id,
    e.meta_dt
  FROM page_with_bots e
)"""
  }

  def buildPageEventMergeSQL(p: Params): String =
    buildPageIncomingSQL(p) +
    s"""

MERGE INTO ${p.ref} t
USING page_incoming s
ON  t.wiki_id       = s.wiki_id
AND t.event_meta_id = s.event_meta_id
WHEN MATCHED AND t.source = 'events' THEN
  UPDATE SET
    t.event_type                                                    = s.event_type,
    t.event_timestamp                                               = s.event_timestamp,
    t.event_user_id                                                 = s.event_user_id,
    t.event_user_central_id                                         = s.event_user_central_id,
    t.event_user_text_historical                                    = s.event_user_text_historical,
    t.event_user_is_anonymous                                       = s.event_user_is_anonymous,
    t.event_user_is_temporary                                       = s.event_user_is_temporary,
    t.event_user_is_permanent                                       = s.event_user_is_permanent,
    t.event_user_registration_timestamp                             = s.event_user_registration_timestamp,

    t.page_title_historical                                         = s.page_title_historical,
    t.page_namespace_historical                                     = s.page_namespace_historical,
    t.page_namespace_is_content_historical                          = s.page_namespace_is_content_historical,
    t.event_user_is_bot_by_historical                               = s.event_user_is_bot_by_historical,
    t.event_user_is_cross_wiki                                      = s.event_user_is_cross_wiki,
    t.page_is_deleted                                               = s.page_is_deleted,
    t.control_map = map_concat(COALESCE(t.control_map, map()),
                               map('page_meta_id',    s.event_meta_id,
                                   'page_update_dt',  CAST(s.meta_dt AS STRING)))
WHEN NOT MATCHED THEN
  INSERT (
    source,
    wiki_id,
    event_meta_id,
    event_entity,
    event_type,
    event_timestamp,
    event_user_id,
    event_user_central_id,
    event_user_text_historical,
    event_user_is_anonymous,
    event_user_is_temporary,
    event_user_is_permanent,
    event_user_registration_timestamp,

    page_id,
    page_title_historical,
    page_namespace_historical,
    revision_id,
    revision_parent_id,
    revision_minor_edit,
    revision_text_bytes,
    revision_text_bytes_diff,
    revision_text_sha1,
    revision_tags,
    revision_deleted_parts,
    page_namespace_is_content_historical,
    event_user_is_bot_by_historical,
    event_user_revision_count,
    event_user_groups_historical,
    user_id,
    user_text_historical,
    user_is_anonymous,
    user_is_temporary,
    user_is_permanent,
    user_groups_historical,
    user_is_bot_by_historical,
    user_is_created_by_self,
    user_is_created_by_system,
    user_is_created_by_peer,
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    event_user_is_cross_wiki,
    page_is_deleted,
    revision_is_deleted_by_page_deletion,
    user_central_id,
    control_map
  ) VALUES (
    s.source,
    s.wiki_id,
    s.event_meta_id,
    s.event_entity,
    s.event_type,
    s.event_timestamp,
    s.event_user_id,
    s.event_user_central_id,
    s.event_user_text_historical,
    s.event_user_is_anonymous,
    s.event_user_is_temporary,
    s.event_user_is_permanent,
    s.event_user_registration_timestamp,

    s.page_id,
    s.page_title_historical,
    s.page_namespace_historical,
    s.revision_id,
    s.revision_parent_id,
    s.revision_minor_edit,
    s.revision_text_bytes,
    s.revision_text_bytes_diff,
    s.revision_text_sha1,
    s.revision_tags,
    s.revision_deleted_parts,
    s.page_namespace_is_content_historical,
    s.event_user_is_bot_by_historical,
    s.event_user_revision_count,
    CAST(NULL AS ARRAY<STRING>),
    CAST(NULL AS BIGINT),
    CAST(NULL AS STRING),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS ARRAY<STRING>),
    CAST(NULL AS ARRAY<STRING>),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    CAST(NULL AS BOOLEAN),
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    s.event_user_is_cross_wiki,
    s.page_is_deleted,
    s.revision_is_deleted_by_page_deletion,
    s.user_central_id,
    map('page_meta_id',   s.event_meta_id,
        'page_update_dt', CAST(s.meta_dt AS STRING))
  )"""

  /**
   * Returns the CTE chain for user events (create, rename, groups_change) from user_change_v1.
   * Tests append "SELECT * FROM user_incoming" to exercise the logic without Iceberg.
   *
   * Field source notes (verified against mediawiki.user_change schema):
   * (1) event_user_* = performer.* (the admin/system performing the change), consistent
   *     with wmf.mediawiki_history where event_user_* is always the actor.
   *     event_user_revision_count is NULL — performer.edit_count is not in the stream.
   * (2) user_* = user.* (the user being created/renamed/altered — the entity).
   *     user_text_historical: user.user_text — post-event username (new name after a rename).
   *     user_groups_historical: user.groups — post-event groups (new groups after altergroups).
   * (3) user_is_created_by_self: true when the performer is the same user as the one being
   *     created (self-registration).
   *     user_is_created_by_system: true for autocreate events (SSO/CentralAuth).
   *     user_is_created_by_peer: true when a distinct admin creates the account.
   *     All three are NULL for non-create events; back-filled by MERGE 6b for rename/altergroups.
   * (4) groups_change mapped to 'altergroups' to match wmf.mediawiki_history vocabulary.
   */
  def buildUserIncomingSQL(p: Params): String =
    s"""WITH

raw_user_events AS (
  SELECT
    wiki_id,
    user_change_kind,
    to_timestamp(dt)               AS event_timestamp,
    meta.id                        AS meta_id,
    to_timestamp(meta.dt)          AS meta_dt,
    is_autocreate,
    user.user_id                   AS user_id,
    user.user_central_id           AS user_central_id,
    user.user_text                 AS user_text,
    user.is_temp                   AS is_temp,
    user.edit_count                AS edit_count,
    user.groups                    AS user_groups,
    user.registration_dt           AS registration_dt,
    performer.user_id              AS performer_user_id,
    performer.user_central_id      AS performer_user_central_id,
    performer.user_text            AS performer_user_text,
    performer.is_temp              AS performer_is_temp,
    performer.registration_dt      AS performer_registration_dt,
    performer.groups               AS performer_groups
  FROM ${p.userChangeTable}
  WHERE year  = ${p.year}
    AND month = ${p.month}
    AND day   = ${p.day}
    AND user_change_kind IN ('create', 'rename', 'groups_change')
),

deduplicated_user AS (
  -- Deduplicate on meta.id (the MERGE 6 join key). meta.id is a delivery UUID with no
  -- repeats within a single day; this guard handles partition re-reads on reruns.
  SELECT * FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY wiki_id, meta_id
        ORDER BY meta_dt DESC
      ) AS rn
    FROM raw_user_events
  )
  WHERE rn = 1
),

user_with_bots AS (
  SELECT
    e.*,
    filter(
      array(
        CASE WHEN lower(e.performer_user_text) RLIKE '(?i)^.*bot([^a-z].*$$|$$)'
             THEN 'name' END,
        CASE WHEN array_contains(COALESCE(e.performer_groups, array()), 'bot')
             THEN 'group' END
      ),
      x -> x IS NOT NULL
    ) AS event_user_is_bot_by_historical,
    filter(
      array(
        CASE WHEN lower(e.user_text) RLIKE '(?i)^.*bot([^a-z].*$$|$$)'
             THEN 'name' END,
        CASE WHEN array_contains(COALESCE(e.user_groups, array()), 'bot')
             THEN 'group' END
      ),
      x -> x IS NOT NULL
    ) AS user_is_bot_by_historical
  FROM deduplicated_user e
),

user_incoming AS (
  SELECT
    'events'                                                            AS source,
    e.wiki_id,
    e.meta_id                                                           AS event_meta_id,
    'user'                                                              AS event_entity,
    CASE e.user_change_kind
      WHEN 'create'        THEN 'create'
      WHEN 'rename'        THEN 'rename'
      WHEN 'groups_change' THEN 'altergroups'
    END                                                                 AS event_type,
    e.event_timestamp,
    -- event_user_* = performer (the admin/system performing the change)
    e.performer_user_id                                                 AS event_user_id,
    e.performer_user_central_id                                         AS event_user_central_id,
    e.performer_user_text                                               AS event_user_text_historical,
    (e.performer_user_id IS NULL)                                       AS event_user_is_anonymous,
    COALESCE(e.performer_is_temp, FALSE)                                AS event_user_is_temporary,
    (e.performer_user_id IS NOT NULL
     AND NOT COALESCE(e.performer_is_temp, FALSE))                      AS event_user_is_permanent,
    to_timestamp(e.performer_registration_dt)                           AS event_user_registration_timestamp,

    CAST(NULL AS BIGINT)                                                AS page_id,
    CAST(NULL AS STRING)                                                AS page_title_historical,
    CAST(NULL AS INT)                                                   AS page_namespace_historical,
    CAST(NULL AS BIGINT)                                                AS revision_id,
    CAST(NULL AS BIGINT)                                                AS revision_parent_id,
    CAST(NULL AS BOOLEAN)                                               AS revision_minor_edit,
    CAST(NULL AS BIGINT)                                                AS revision_text_bytes,
    CAST(NULL AS BIGINT)                                                AS revision_text_bytes_diff,
    CAST(NULL AS STRING)                                                AS revision_text_sha1,
    CAST(NULL AS ARRAY<STRING>)                                         AS revision_tags,
    CAST(NULL AS ARRAY<STRING>)                                         AS revision_deleted_parts,
    CAST(NULL AS BOOLEAN)                                               AS page_namespace_is_content_historical,
    e.event_user_is_bot_by_historical,
    CAST(NULL AS BIGINT)                                                AS event_user_revision_count,
    e.performer_groups                                                  AS event_user_groups_historical,
    -- user_* = the user being created/renamed/altered (the entity)
    e.user_id                                                           AS user_id,
    e.user_central_id                                                   AS user_central_id,
    e.user_text                                                         AS user_text_historical,
    (e.user_id IS NULL)                                                 AS user_is_anonymous,
    COALESCE(e.is_temp, FALSE)                                          AS user_is_temporary,
    (e.user_id IS NOT NULL
     AND NOT COALESCE(e.is_temp, FALSE))                                AS user_is_permanent,
    e.user_groups                                                       AS user_groups_historical,
    e.user_is_bot_by_historical,
    CASE WHEN e.user_change_kind = 'create'
         THEN (NOT COALESCE(e.is_autocreate, FALSE)
               AND COALESCE(e.performer_user_id = e.user_id, FALSE))
         ELSE NULL
    END                                                                 AS user_is_created_by_self,
    CASE WHEN e.user_change_kind = 'create'
         THEN COALESCE(e.is_autocreate, FALSE)
         ELSE NULL
    END                                                                 AS user_is_created_by_system,
    CASE WHEN e.user_change_kind = 'create'
         THEN (NOT COALESCE(e.is_autocreate, FALSE)
               AND NOT COALESCE(e.performer_user_id = e.user_id, FALSE))
         ELSE NULL
    END                                                                 AS user_is_created_by_peer,
    CAST(NULL AS BOOLEAN)                                               AS revision_is_identity_reverted,
    CAST(NULL AS BIGINT)                                                AS revision_first_identity_reverting_revision_id,
    CAST(NULL AS BIGINT)                                                AS revision_seconds_to_identity_revert,
    CAST(NULL AS BOOLEAN)                                               AS revision_is_identity_revert,
    (e.performer_user_text LIKE '%>%'
     AND (e.performer_user_id IS NULL)
     AND NOT COALESCE(e.performer_is_temp, FALSE))                      AS event_user_is_cross_wiki,
    CAST(NULL AS BOOLEAN)                                               AS page_is_deleted,
    CAST(NULL AS BOOLEAN)                                               AS revision_is_deleted_by_page_deletion,
    e.meta_dt
  FROM user_with_bots e
)"""

  def buildUserEventMergeSQL(p: Params): String =
    buildUserIncomingSQL(p) +
    s"""

MERGE INTO ${p.ref} t
USING user_incoming s
ON  t.wiki_id       = s.wiki_id
AND t.event_meta_id = s.event_meta_id
WHEN MATCHED AND t.source = 'events' THEN
  UPDATE SET
    t.event_type                                                    = s.event_type,
    t.event_timestamp                                               = s.event_timestamp,
    t.event_user_id                                                 = s.event_user_id,
    t.event_user_central_id                                         = s.event_user_central_id,
    t.event_user_text_historical                                    = s.event_user_text_historical,
    t.event_user_is_anonymous                                       = s.event_user_is_anonymous,
    t.event_user_is_temporary                                       = s.event_user_is_temporary,
    t.event_user_is_permanent                                       = s.event_user_is_permanent,
    t.event_user_registration_timestamp                             = s.event_user_registration_timestamp,

    t.event_user_revision_count                                     = s.event_user_revision_count,
    t.event_user_is_bot_by_historical                               = s.event_user_is_bot_by_historical,
    t.event_user_groups_historical                                  = s.event_user_groups_historical,
    t.user_id                                                       = s.user_id,
    t.user_text_historical                                          = s.user_text_historical,
    t.user_is_anonymous                                             = s.user_is_anonymous,
    t.user_is_temporary                                             = s.user_is_temporary,
    t.user_is_permanent                                             = s.user_is_permanent,
    t.user_groups_historical                                        = s.user_groups_historical,
    t.user_is_bot_by_historical                                     = s.user_is_bot_by_historical,
    t.user_is_created_by_self                                       = s.user_is_created_by_self,
    t.user_is_created_by_system                                     = s.user_is_created_by_system,
    t.user_is_created_by_peer                                       = s.user_is_created_by_peer,
    t.user_central_id                                               = s.user_central_id,
    t.event_user_is_cross_wiki                                      = s.event_user_is_cross_wiki,
    t.control_map = map_concat(COALESCE(t.control_map, map()),
                               map('user_meta_id',   s.event_meta_id,
                                   'user_update_dt', CAST(s.meta_dt AS STRING)))
WHEN NOT MATCHED THEN
  INSERT (
    source,
    wiki_id,
    event_meta_id,
    event_entity,
    event_type,
    event_timestamp,
    event_user_id,
    event_user_central_id,
    event_user_text_historical,
    event_user_is_anonymous,
    event_user_is_temporary,
    event_user_is_permanent,
    event_user_registration_timestamp,

    page_id,
    page_title_historical,
    page_namespace_historical,
    revision_id,
    revision_parent_id,
    revision_minor_edit,
    revision_text_bytes,
    revision_text_bytes_diff,
    revision_text_sha1,
    revision_tags,
    revision_deleted_parts,
    page_namespace_is_content_historical,
    event_user_is_bot_by_historical,
    event_user_revision_count,
    event_user_groups_historical,
    user_id,
    user_text_historical,
    user_is_anonymous,
    user_is_temporary,
    user_is_permanent,
    user_groups_historical,
    user_is_bot_by_historical,
    user_is_created_by_self,
    user_is_created_by_system,
    user_is_created_by_peer,
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    event_user_is_cross_wiki,
    page_is_deleted,
    revision_is_deleted_by_page_deletion,
    user_central_id,
    control_map
  ) VALUES (
    s.source,
    s.wiki_id,
    s.event_meta_id,
    s.event_entity,
    s.event_type,
    s.event_timestamp,
    s.event_user_id,
    s.event_user_central_id,
    s.event_user_text_historical,
    s.event_user_is_anonymous,
    s.event_user_is_temporary,
    s.event_user_is_permanent,
    s.event_user_registration_timestamp,

    s.page_id,
    s.page_title_historical,
    s.page_namespace_historical,
    s.revision_id,
    s.revision_parent_id,
    s.revision_minor_edit,
    s.revision_text_bytes,
    s.revision_text_bytes_diff,
    s.revision_text_sha1,
    s.revision_tags,
    s.revision_deleted_parts,
    s.page_namespace_is_content_historical,
    s.event_user_is_bot_by_historical,
    s.event_user_revision_count,
    s.event_user_groups_historical,
    s.user_id,
    s.user_text_historical,
    s.user_is_anonymous,
    s.user_is_temporary,
    s.user_is_permanent,
    s.user_groups_historical,
    s.user_is_bot_by_historical,
    s.user_is_created_by_self,
    s.user_is_created_by_system,
    s.user_is_created_by_peer,
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    s.event_user_is_cross_wiki,
    s.page_is_deleted,
    s.revision_is_deleted_by_page_deletion,
    s.user_central_id,
    map('user_meta_id',   s.event_meta_id,
        'user_update_dt', CAST(s.meta_dt AS STRING))
  )"""

  /**
   * MERGE 6: back-patches page_is_deleted and revision_is_deleted_by_page_deletion on existing
   * revision rows when today's page events include a delete or undelete.
   * Joins on (wiki_id, page_id) — NOT the partition key — so this scans all monthly partitions.
   * Fires unconditionally; Spark prunes the MERGE to a no-op when the source CTE is empty.
   */
  def buildPageDeletionBackpatchSQL(p: Params): String = {
    buildPageIncomingSQL(p) +
    s""",

page_deletion_events AS (
  -- Last delete/undelete per page for the day; TRUE = page was deleted, FALSE = restored.
  SELECT wiki_id, page_id, is_delete, meta_dt
  FROM (
    SELECT
      wiki_id,
      page_id,
      (event_type = 'delete') AS is_delete,
      meta_dt,
      row_number() OVER (PARTITION BY wiki_id, page_id ORDER BY event_timestamp DESC) AS rn
    FROM page_incoming
    WHERE event_type IN ('delete', 'restore')
  )
  WHERE rn = 1
)

MERGE INTO ${p.ref} t
USING (SELECT /*+ BROADCAST */ * FROM page_deletion_events) s
ON  t.wiki_id      = s.wiki_id
AND t.page_id      = s.page_id
AND t.event_entity = 'revision'
WHEN MATCHED THEN UPDATE SET
  t.page_is_deleted                      = s.is_delete,
  t.revision_is_deleted_by_page_deletion = s.is_delete,
  t.control_map = map_concat(COALESCE(t.control_map, map()), map('page_deletion_dt', CAST(s.meta_dt AS STRING)))"""
  }

  /**
   * Returns SQL that back-fills user_is_created_by_* on rename/altergroups rows inserted
   * by MERGE 6, by joining them to the corresponding create-event row already in the target
   * table. Runs as a separate MERGE so it can read the target after MERGE 6 has committed.
   */
  def buildUserCreationProvenanceBackfillSQL(p: Params): String =
    buildUserIncomingSQL(p) +
    s""",

user_provenance AS (
  -- GROUP BY + MAX collapses to one row per (wiki_id, user_id), which is required by
  -- Iceberg MERGE (each target row must match at most one source row). In practice a
  -- user has exactly one create event, so MAX is a no-op aggregate.
  SELECT
    wiki_id,
    user_id,
    MAX(user_is_created_by_self)   AS user_is_created_by_self,
    MAX(user_is_created_by_system) AS user_is_created_by_system,
    MAX(user_is_created_by_peer)   AS user_is_created_by_peer
  FROM ${p.ref}
  WHERE event_entity = 'user'
    AND event_type   = 'create'
  GROUP BY wiki_id, user_id
)

MERGE INTO ${p.ref} t
USING (
  SELECT
    ui.wiki_id,
    ui.event_meta_id,
    up.user_is_created_by_self,
    up.user_is_created_by_system,
    up.user_is_created_by_peer
  FROM user_incoming ui
  JOIN user_provenance up
    ON  ui.wiki_id = up.wiki_id
    AND ui.user_id = up.user_id
  WHERE ui.event_type IN ('rename', 'altergroups')
) s
ON  t.wiki_id       = s.wiki_id
AND t.event_meta_id = s.event_meta_id
WHEN MATCHED THEN UPDATE SET
  t.user_is_created_by_self   = s.user_is_created_by_self,
  t.user_is_created_by_system = s.user_is_created_by_system,
  t.user_is_created_by_peer   = s.user_is_created_by_peer"""
}
