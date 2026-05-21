package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Daily delta writer for mediawiki_history_incremental_v1.
 *
 * Reads mediawiki.page_change events for one calendar day, maps them to the
 * narrow schema, computes two-tier 90d revert detection, applies namespace-content
 * and bot classification, then MERGE INTOs the Iceberg target.
 *
 * Two-tier revert fields (8 total):
 *   Bounded tier (_within_90_days): always populated as true or false.
 *   Authoritative tier (bare names): true when detected within 90d, NULL otherwise.
 *   Both tiers are always written together — NULL/true is structurally impossible.
 *
 * Field paths are verified against mediawiki.page_change.v1 schema at
 * ~/wmf/gitlab/schemas-event-primary/jsonschema/mediawiki/page/change/latest.yaml
 *
 * Natural key for MERGE:
 *   Revision events: (wiki_id, revision_id) — immutable and unique per wiki.
 *   Page events: (wiki_id, event_meta_id) where event_meta_id = meta.id (UUID).
 *     This key applies only to source='events' rows; event_meta_id is NULL for all
 *     source='snapshot' rows (see event_meta_id column note below).
 *     revision_id is NULL for page events in wmf.mediawiki_history (confirmed via
 *     MediawikiEvent.fromPageState), so revision_id cannot serve as a page-event key.
 *     The composite timestamp key (wiki_id, page_id, event_type, event_timestamp) was
 *     ruled out: April 2026 data shows 10 move collisions at second-precision timestamps.
 *     meta.id has zero repeats across a full month of events (empirically reliable).
 *     Note: meta.id is a delivery UUID, not a business key — a stream replay could produce
 *     a new UUID for the same logical event. Empirically this has not been observed;
 *     control_map['page_update_dt'] guards against stale rewrites on normal reruns.
 *     TODO: when event_entity='user' is implemented, event_meta_id may be reused there
 *     with the same meta.id approach.
 *
 * Two schema columns support provenance tracking:
 *   event_meta_id STRING: MERGE key for page events. Populated from meta.id for
 *     source='events' page rows; NULL for all revision rows and all snapshot rows.
 *   control_map MAP<STRING,STRING>: per-stream update timestamps for rerun guards.
 *     Keys: revision_update_dt (MERGE 1), revert_patch_dt (MERGE 2),
 *           tags_update_dt (MERGE 3), visibility_update_dt (MERGE 4),
 *           page_meta_id + page_update_dt (MERGE 5).
 *     Each MERGE's WHEN MATCHED UPDATE uses map_concat to add/overwrite only its own
 *     key(s), preserving entries written by other MERGEs. Timestamps stored as strings
 *     (ISO-8601 UTC); lexicographic order equals chronological order.
 *     TODO (follow-up): convert control_map to a typed struct for better schema enforcement.
 *
 * Five MERGEs are required because each operation has a different join key and
 * source table that cannot be combined without violating Iceberg's one-source-row-
 * per-target-row constraint or sacrificing partition pruning:
 *
 *   MERGE 1 (buildMergeSQL): insert/update today's revision events.
 *     ON t.wiki_id = s.wiki_id AND t.revision_id = s.revision_id
 *     WHEN MATCHED AND t.source = 'events' THEN UPDATE
 *     WHEN NOT MATCHED THEN INSERT
 *     source='snapshot' rows that match the ON clause are intentionally left untouched:
 *     in SQL MERGE, a row that matches ON but satisfies no WHEN MATCHED condition is
 *     skipped — it does NOT fall through to NOT MATCHED. This prevents duplicates when
 *     a revision already exists as source='snapshot' (e.g. from MWHistorySnapshotMerger).
 *
 *   MERGE 2 (buildBackPatchSQL): fix revert fields on rows already in the target
 *     whose first revert arrived in today's batch.
 *     ON t.revision_id = s.target_revision_id   (no source filter)
 *     s.target_revision_id is the REVERTED revision — a different row from the
 *     reverting revision. No t.source filter because the reverted row may be
 *     source='events' or source='snapshot'.
 *
 *   MERGE 3 (buildTagsMergeSQL): update revision_tags from today's revision_tags_change events.
 *     ON t.wiki_id = s.wiki_id AND t.revision_id = s.revision_id
 *        AND t.event_timestamp >= today - 90 DAYS
 *     Source: event.mediawiki_revision_tags_change (separate stream from page_change_v1,
 *     which does not carry tags). The 90-day lower bound on the target enables Iceberg
 *     partition pruning — histogram analysis on April 2026 data shows 99.8% of tag-change
 *     events target revisions <90 days old; the 0.2% tail is caught by the monthly
 *     snapshot merger. No INSERT clause: a tag-change event alone does not create a row.
 *
 *   MERGE 4 (buildVisibilityMergeSQL): update revision_deleted_parts from today's
 *     revision_visibility_change events.
 *     ON t.wiki_id = s.wiki_id AND t.revision_id = s.revision_id
 *     Source: event.mediawiki_revision_visibility_change. No lower bound on event_timestamp:
 *     visibility changes target all-time revisions (46% are >1 year old per 2026-06 analysis),
 *     so partition pruning is sacrificed in exchange for full coverage.
 *     No INSERT clause: a visibility-change event alone does not create a row.
 *
 *   MERGE 5 (buildPageEventMergeSQL): insert/update today's page events (move, delete, undelete).
 *     ON t.wiki_id = s.wiki_id AND t.event_meta_id = s.event_meta_id
 *     Actor from performer (not revision.editor). Event timestamp from top-level dt.
 *     Historical title/namespace from page.* for all event kinds (confirmed by sampling MWH:
 *     page_title_historical for move events = post-move title, e.g. "Half-life_(physics)").
 *     Namespace prefix stripped (page_change_v1 includes "User:", "Talk:", etc. in page_title;
 *     wmf.mediawiki_history stores the bare title with namespace in page_namespace_historical).
 *     undelete mapped to 'restore' to match wmf.mediawiki_history vocabulary.
 *     All revision-specific fields (revision_id, revert tiers, etc.) written as NULL.
 *     visibility_change kind is excluded: those reach the target via MERGE 4's dedicated stream.
 *
 * A single MERGE with a UNION'd source and compound OR join is not viable:
 * Iceberg requires each target row to match at most one source row, and the
 * compound OR prevents partition pruning. The five-MERGE approach keeps each
 * join key simple and the affected target row sets provably disjoint.
 */
object MWHistoryDeltaWriter {

  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  case class Params(
    sourceTable: String        = "",  // e.g. event.mediawiki_page_change
    targetTable: String        = "",  // mediawiki_history_incremental_v1
    namespacesTable: String    = "",  // wmf_raw.mediawiki_project_namespace_map
    namespacesSnapshot: String = "",  // YYYY-MM snapshot for the namespaces table
    tagsTable: String          = "",  // e.g. event.mediawiki_revision_tags_change
    visibilityTable: String    = "",  // e.g. event.mediawiki_revision_visibility_change
    year: Int  = 0,
    month: Int = 0,
    day: Int   = 0
  )

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
      opt[String]("source_table").required().action((v, p) => p.copy(sourceTable = v))
      opt[String]("target_table").required().action((v, p) => p.copy(targetTable = v))
      opt[String]("namespaces_table").required().action((v, p) => p.copy(namespacesTable = v))
      opt[String]("namespaces_snapshot").required().action((v, p) => p.copy(namespacesSnapshot = v))
      opt[String]("tags_table").required().action((v, p) => p.copy(tagsTable = v))
      opt[String]("visibility_table").required().action((v, p) => p.copy(visibilityTable = v))
      opt[Int]("year").required().action((v, p) => p.copy(year = v))
      opt[Int]("month").required().action((v, p) => p.copy(month = v))
      opt[Int]("day").required().action((v, p) => p.copy(day = v))
    }
    parser.parse(args, Params()).getOrElse(sys.exit(1))
  }

  def run(spark: SparkSession, p: Params): Unit = {
    log.info(s"MWHistoryDeltaWriter params: $p")
    // MERGE 1: insert/update today's source='events' rows.
    val mergeSql = buildMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter MERGE INTO ${p.targetTable}:\n$mergeSql")
    spark.sql(mergeSql).collect()
    // TODO: stage MERGE 1 + MERGE 2 on an Iceberg branch and commit the branch
    //   atomically, eliminating the window where MERGE 1 has committed but the
    //   back-patch has not. Requires WAP (write-audit-publish) branch support.
    // MERGE 2: back-patch revert fields on existing rows (events or snapshot) that
    // were first-reverted by a revision in today's batch. Separate MERGE because the
    // join key is the REVERTED revision_id, not the incoming revision_id.
    val backPatchSql = buildBackPatchSQL(p)
    log.info(s"Running MWHistoryDeltaWriter back-patch MERGE INTO ${p.targetTable}:\n$backPatchSql")
    spark.sql(backPatchSql).collect()
    // MERGE 3: update revision_tags from today's revision_tags_change events.
    // Separate stream from page_change_v1; tags are applied asynchronously after revision creation.
    val tagsSql = buildTagsMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter tags MERGE INTO ${p.targetTable}:\n$tagsSql")
    spark.sql(tagsSql).collect()
    // MERGE 4: update revision_deleted_parts from today's revision_visibility_change events.
    // Visibility changes are applied independently of edits; the 90-day lower bound on the
    // target matches the tags window and enables partition pruning.
    val visibilitySql = buildVisibilityMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter visibility MERGE INTO ${p.targetTable}:\n$visibilitySql")
    spark.sql(visibilitySql).collect()
    // MERGE 5: insert/update today's page events (move, delete, undelete).
    // Keyed on (wiki_id, event_meta_id) where event_meta_id = meta.id. revision_id is NULL
    // for all page events (matches wmf.mediawiki_history convention).
    val pageEventSql = buildPageEventMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter page events MERGE INTO ${p.targetTable}:\n$pageEventSql")
    spark.sql(pageEventSql).collect()
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
    CAST(NULL AS BOOLEAN)                                           AS event_user_is_created_by_self,
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
  FROM ${p.sourceTable}
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
  -- page_namespace_is_content_historical is null when the namespace is not in the map,
  -- which can happen for wikis not yet in the snapshot or for unusual namespace IDs.
  SELECT
    e.*,
    (ns.namespace_is_content = 1) AS page_namespace_is_content_historical
  FROM deduplicated e
  LEFT JOIN ${p.namespacesTable} ns
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
  -- Pull the last 90 days from the target table regardless of source: a revert can target
  -- a revision that landed via the snapshot merger as well as one from a prior events run.
  --
  -- Rows with sha1_rank > 1 (already-known reverts) are intentionally included.
  -- They must be here so that sha1_rank is computed correctly over the full history:
  -- if the seed already has sha1_rank=1 (base) and sha1_rank=2 (first revert) for sha1=X,
  -- a new incoming sha1=X row is sha1_rank=3, not sha1_rank=2.  If we excluded sha1_rank>1
  -- seed rows, the incoming row would get sha1_rank=2 and trigger a spurious back_patch
  -- that overwrites the base row's already-correct first_reverting_rev_id.
  SELECT
    wiki_id,
    page_id,
    revision_id,
    revision_text_sha1,
    event_timestamp,
    FALSE AS is_incoming
  FROM ${p.targetTable}
  WHERE revision_text_sha1 IS NOT NULL
    AND event_timestamp  >= TIMESTAMP '${p.year}-${paddedMonth}-${paddedDay} 00:00:00' - INTERVAL 90 DAYS
),

revert_candidates AS (
  SELECT wiki_id, page_id, revision_id, revision_text_sha1, event_timestamp, TRUE AS is_incoming
  FROM with_bots
  WHERE revision_text_sha1 IS NOT NULL
  UNION ALL
  SELECT * FROM revert_seed
),

sha1_ranked AS (
  -- Rank occurrences of each (wiki_id, page_id, sha1) by time to find base and revert.
  SELECT
    *,
    row_number() OVER (
      PARTITION BY wiki_id, page_id, revision_text_sha1
      ORDER BY event_timestamp, revision_id
    )                     AS sha1_rank,
    MIN(event_timestamp) OVER (
      PARTITION BY wiki_id, page_id, revision_text_sha1
    )                     AS sha1_first_ts
  FROM revert_candidates
),

first_revert AS (
  -- The second occurrence of a sha1 is the first reverting revision.
  -- sha1_rank is a row_number(), so sha1_rank = 2 is unique per (wiki_id, page_id, sha1)
  -- group.  This CTE therefore has exactly one row per sha1 group.  The LEFT JOIN in
  -- revert_annotations is thus 1-to-1 regardless of how many incoming revisions share
  -- the same sha1: the join delivers the correct first reverter to sha1_rank=1 rows
  -- and harmlessly delivers the same value to sha1_rank>1 rows (where it is unused).
  SELECT
    wiki_id,
    page_id,
    revision_text_sha1,
    revision_id       AS first_reverting_rev_id,
    event_timestamp   AS first_revert_ts
  FROM sha1_ranked
  WHERE sha1_rank = 2
),

revert_annotations AS (
  -- Annotates only incoming rows (WHERE r.is_incoming below).
  --
  -- sha1_rank > 1 → this revision IS a reverting revision (the common case for incoming rows).
  -- sha1_rank = 1 → this revision IS the base (reverted candidate).  For incoming rows this
  --   fires only for late-arriving events: a revision whose rev_dt falls within the 90-day
  --   window but is older than all same-sha1 seed rows.  Normal edits (rev_dt ≈ today) will
  --   find an existing base in the seed and receive sha1_rank ≥ 2.
  --
  -- Not computed: the "compound revert" case from DenormalizedRevisionsBuilder — a reverting
  -- revision that is itself identity-reverted by a later wider revert of a DIFFERENT sha1.
  -- Detecting this requires cross-sha1-group reasoning that cannot be expressed in the
  -- per-sha1 partition used here.  The monthly SnapshotMerger corrects these fields via the
  -- full Spark-based algorithm.
  SELECT
    r.wiki_id,
    r.revision_id,
    -- Bounded tier: true or false, always populated.
    (r.sha1_rank > 1
     AND r.event_timestamp <= r.sha1_first_ts + INTERVAL 90 DAYS)       AS revision_is_identity_revert_within_90_days,
    (r.sha1_rank = 1
     AND fr.first_revert_ts IS NOT NULL
     AND fr.first_revert_ts <= r.event_timestamp + INTERVAL 90 DAYS)    AS revision_is_identity_reverted_within_90_days,
    CASE WHEN r.sha1_rank = 1
              AND fr.first_revert_ts IS NOT NULL
              AND fr.first_revert_ts <= r.event_timestamp + INTERVAL 90 DAYS
         THEN fr.first_reverting_rev_id
    END                                                                   AS revision_first_identity_reverting_revision_id_within_90_days,
    CASE WHEN r.sha1_rank = 1
              AND fr.first_revert_ts IS NOT NULL
              AND fr.first_revert_ts <= r.event_timestamp + INTERVAL 90 DAYS
         THEN unix_timestamp(fr.first_revert_ts) - unix_timestamp(r.event_timestamp)
    END                                                                   AS revision_seconds_to_identity_revert_within_90_days,
    -- Authoritative tier: TRUE when detected within 90d, NULL otherwise.
    -- Both tiers are always written together — NULL/true is impossible.
    CASE WHEN r.sha1_rank > 1
              AND r.event_timestamp <= r.sha1_first_ts + INTERVAL 90 DAYS
         THEN TRUE
    END                                                                   AS revision_is_identity_revert,
    CASE WHEN r.sha1_rank = 1
              AND fr.first_revert_ts IS NOT NULL
              AND fr.first_revert_ts <= r.event_timestamp + INTERVAL 90 DAYS
         THEN TRUE
    END                                                                   AS revision_is_identity_reverted,
    CASE WHEN r.sha1_rank = 1
              AND fr.first_revert_ts IS NOT NULL
              AND fr.first_revert_ts <= r.event_timestamp + INTERVAL 90 DAYS
         THEN fr.first_reverting_rev_id
    END                                                                   AS revision_first_identity_reverting_revision_id,
    CASE WHEN r.sha1_rank = 1
              AND fr.first_revert_ts IS NOT NULL
              AND fr.first_revert_ts <= r.event_timestamp + INTERVAL 90 DAYS
         THEN unix_timestamp(fr.first_revert_ts) - unix_timestamp(r.event_timestamp)
    END                                                                   AS revision_seconds_to_identity_revert
  FROM sha1_ranked r
  LEFT JOIN first_revert fr
    ON  r.wiki_id            = fr.wiki_id
    AND r.page_id            = fr.page_id
    AND r.revision_text_sha1 = fr.revision_text_sha1
  WHERE r.is_incoming
),

incoming AS (
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
    e.event_user_is_created_by_self,
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
    -- Authoritative tier: TRUE when detected, NULL otherwise (not false).
    ra.revision_is_identity_reverted,
    ra.revision_first_identity_reverting_revision_id,
    ra.revision_seconds_to_identity_revert,
    ra.revision_is_identity_revert,
    -- Bounded tier: always true or false, never NULL.
    COALESCE(ra.revision_is_identity_reverted_within_90_days,                 FALSE) AS revision_is_identity_reverted_within_90_days,
    ra.revision_first_identity_reverting_revision_id_within_90_days,
    ra.revision_seconds_to_identity_revert_within_90_days,
    COALESCE(ra.revision_is_identity_revert_within_90_days,                   FALSE) AS revision_is_identity_revert_within_90_days,
    e.meta_dt
  FROM with_bots e
  LEFT JOIN revert_annotations ra
    ON  e.wiki_id     = ra.wiki_id
    AND e.revision_id = ra.revision_id
)"""
  }

  /**
   * Returns the CTE chain for the back-patch source, without the MERGE.
   * Tests append "SELECT * FROM back_patch" to exercise the logic without Iceberg.
   *
   * back_patch contains the target_revision_id of seed rows (already in the target)
   * that were first-reverted by a revision in today's incoming batch.
   *
   * TODO (follow-up patch): extract the shared sha1-ranking logic into a helper and
   *   reuse it here and in buildIncomingSQL; also add deduplication to incoming_sha1s
   *   to match the deduplicated CTE in the insert/update path.
   */
  def buildBackPatchCteSQL(p: Params): String = {
    val paddedMonth = "%02d".format(p.month)
    val paddedDay   = "%02d".format(p.day)

    s"""WITH incoming_sha1s AS (
  SELECT
    wiki_id,
    page.page_id                  AS page_id,
    revision.rev_id               AS revision_id,
    revision.rev_sha1             AS revision_text_sha1,
    to_timestamp(revision.rev_dt) AS event_timestamp,
    to_timestamp(meta.dt)         AS meta_dt
  FROM ${p.sourceTable}
  WHERE year  = ${p.year}
    AND month = ${p.month}
    AND day   = ${p.day}
    AND page_change_kind IN ('create', 'edit')
    AND revision.rev_sha1 IS NOT NULL
),

seed_sha1s AS (
  SELECT wiki_id, page_id, revision_id, revision_text_sha1, event_timestamp,
         CAST(NULL AS TIMESTAMP) AS meta_dt
  FROM ${p.targetTable}
  WHERE revision_text_sha1 IS NOT NULL
    AND event_timestamp >= TIMESTAMP '${p.year}-${paddedMonth}-${paddedDay} 00:00:00' - INTERVAL 90 DAYS
),

all_sha1s AS (
  SELECT *, TRUE  AS is_incoming FROM incoming_sha1s
  UNION ALL
  SELECT *, FALSE AS is_incoming FROM seed_sha1s
),

sha1_ranked AS (
  SELECT *,
    row_number() OVER (
      PARTITION BY wiki_id, page_id, revision_text_sha1
      ORDER BY event_timestamp, revision_id
    ) AS sha1_rank
  FROM all_sha1s
),

back_patch AS (
  -- Seed rows (already in target) whose first revert arrived in today's incoming batch.
  -- The back-patch updates both revert tiers on the reverted row regardless of its source.
  -- meta_dt comes from the reverting (incoming) revision for the control_map update.
  SELECT
    base.wiki_id,
    base.revision_id                                                              AS target_revision_id,
    revert.revision_id                                                            AS first_reverting_rev_id,
    CAST(unix_timestamp(revert.event_timestamp)
         - unix_timestamp(base.event_timestamp) AS BIGINT)                       AS seconds_to_revert,
    revert.meta_dt                                                                AS meta_dt
  FROM sha1_ranked base
  JOIN sha1_ranked revert
    ON  base.wiki_id            = revert.wiki_id
    AND base.page_id            = revert.page_id
    AND base.revision_text_sha1 = revert.revision_text_sha1
    AND revert.sha1_rank        = 2
  WHERE base.sha1_rank    = 1
    AND NOT base.is_incoming
    AND revert.is_incoming
    AND revert.event_timestamp <= base.event_timestamp + INTERVAL 90 DAYS
)"""
  }

  def buildMergeSQL(p: Params): String =
    buildIncomingSQL(p) +
    s"""

MERGE INTO ${p.targetTable} t
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
    t.event_user_is_created_by_self                                 = s.event_user_is_created_by_self,
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
    t.revision_is_identity_reverted_within_90_days                  = s.revision_is_identity_reverted_within_90_days,
    t.revision_first_identity_reverting_revision_id_within_90_days  = s.revision_first_identity_reverting_revision_id_within_90_days,
    t.revision_seconds_to_identity_revert_within_90_days            = s.revision_seconds_to_identity_revert_within_90_days,
    t.revision_is_identity_revert_within_90_days                    = s.revision_is_identity_revert_within_90_days,
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
    event_user_is_created_by_self,
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
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    revision_is_identity_reverted_within_90_days,
    revision_first_identity_reverting_revision_id_within_90_days,
    revision_seconds_to_identity_revert_within_90_days,
    revision_is_identity_revert_within_90_days,
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
    s.event_user_is_created_by_self,
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
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    s.revision_is_identity_reverted_within_90_days,
    s.revision_first_identity_reverting_revision_id_within_90_days,
    s.revision_seconds_to_identity_revert_within_90_days,
    s.revision_is_identity_revert_within_90_days,
    map('revision_update_dt', CAST(s.meta_dt AS STRING))
  )"""

  def buildBackPatchSQL(p: Params): String =
    buildBackPatchCteSQL(p) +
    s"""

MERGE INTO ${p.targetTable} t
USING back_patch s
ON  t.wiki_id     = s.wiki_id
AND t.revision_id = s.target_revision_id
WHEN MATCHED THEN
  UPDATE SET
    t.revision_is_identity_reverted                                = TRUE,
    t.revision_first_identity_reverting_revision_id                = s.first_reverting_rev_id,
    t.revision_seconds_to_identity_revert                          = s.seconds_to_revert,
    t.revision_is_identity_reverted_within_90_days                 = TRUE,
    t.revision_first_identity_reverting_revision_id_within_90_days = s.first_reverting_rev_id,
    t.revision_seconds_to_identity_revert_within_90_days           = s.seconds_to_revert,
    t.control_map = map_concat(COALESCE(t.control_map, map()), map('revert_patch_dt', CAST(s.meta_dt AS STRING)))"""

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

MERGE INTO ${p.targetTable} t
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
MERGE INTO ${p.targetTable} t
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
   * (7) event_user_is_created_by_self is NULL — not derivable from performer for page events.
   * (8) visibility_change kind is excluded: those reach the target via MERGE 4's dedicated stream.
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
    CAST(NULL AS BOOLEAN)                                               AS event_user_is_created_by_self,
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
    performer.groups                                                    AS user_groups_raw
  FROM ${p.sourceTable}
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
  SELECT
    e.*,
    (ns.namespace_is_content = 1) AS page_namespace_is_content_historical
  FROM deduplicated_page e
  LEFT JOIN ${p.namespacesTable} ns
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
    e.event_user_is_created_by_self,
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
    CAST(NULL AS BOOLEAN)                                               AS revision_is_identity_reverted_within_90_days,
    CAST(NULL AS BIGINT)                                                AS revision_first_identity_reverting_revision_id_within_90_days,
    CAST(NULL AS BIGINT)                                                AS revision_seconds_to_identity_revert_within_90_days,
    CAST(NULL AS BOOLEAN)                                               AS revision_is_identity_revert_within_90_days,
    e.meta_dt
  FROM page_with_bots e
)"""
  }

  def buildPageEventMergeSQL(p: Params): String =
    buildPageIncomingSQL(p) +
    s"""

MERGE INTO ${p.targetTable} t
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
    t.event_user_is_created_by_self                                 = s.event_user_is_created_by_self,
    t.page_title_historical                                         = s.page_title_historical,
    t.page_namespace_historical                                     = s.page_namespace_historical,
    t.page_namespace_is_content_historical                          = s.page_namespace_is_content_historical,
    t.event_user_is_bot_by_historical                               = s.event_user_is_bot_by_historical,
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
    event_user_is_created_by_self,
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
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    revision_is_identity_reverted_within_90_days,
    revision_first_identity_reverting_revision_id_within_90_days,
    revision_seconds_to_identity_revert_within_90_days,
    revision_is_identity_revert_within_90_days,
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
    s.event_user_is_created_by_self,
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
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    s.revision_is_identity_reverted_within_90_days,
    s.revision_first_identity_reverting_revision_id_within_90_days,
    s.revision_seconds_to_identity_revert_within_90_days,
    s.revision_is_identity_revert_within_90_days,
    map('page_meta_id',   s.event_meta_id,
        'page_update_dt', CAST(s.meta_dt AS STRING))
  )"""
}
