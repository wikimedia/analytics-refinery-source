package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.wikimedia.analytics.refinery.job.incremental.mediawikihistory.MWHistoryDeltaWriter.Params

object MWHistoryDeltaPageSQL {


    /**
     * Returns the CTE chain for page events (create, move, delete, undelete) from page_change_v1.
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
     * (4) undelete mapped to 'restore' and create mapped to 'create-page' to match wmf.mediawiki_history vocabulary.
     * (5) revision_id is NULL — page events have no revision in wmf.mediawiki_history.
     * (6) All revert tier fields are NULL — not applicable to page-level events.
     *     Bounded tier is also NULL (not FALSE) unlike revision events.
     * (7) visibility_change kind is excluded: those reach the target via MERGE 4's dedicated stream.
     */
    def buildPageEventSQL(p: Params): String = {
        s"""WITH raw_page_events AS (
  SELECT
    wiki_id,
    meta.id                                                             AS event_meta_id,
    to_timestamp(meta.dt)                                               AS meta_dt,
    'page'                                                              AS event_entity,
    CASE WHEN page_change_kind = 'undelete' THEN 'restore'
         WHEN page_change_kind = 'create' THEN 'create-page'
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
    performer.groups                                                    AS event_user_groups_historical,

    page.page_id                                                        AS page_id,
    CASE WHEN page.namespace_id = 0 THEN page.page_title
         ELSE REGEXP_REPLACE(page.page_title, '^[^:]+:', '')
    END                                                                 AS page_title_historical,
    page.namespace_id                                                   AS page_namespace_historical,
    CASE WHEN page_change_kind = 'delete' THEN TRUE ELSE FALSE END      AS page_is_deleted

  FROM ${p.pageChangeTable}
  WHERE year  = ${p.year}
    AND month = ${p.month}
    AND day   = ${p.day}
    AND page_change_kind IN ('create', 'move', 'delete', 'undelete')
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
        PARTITION BY wiki_id, event_meta_id
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
        CASE WHEN array_contains(e.event_user_groups_historical, 'bot')
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

    e.event_entity,
    e.event_type,
    e.event_timestamp,

    e.event_user_id,
    e.event_user_central_id,
    e.event_user_text_historical,
    e.event_user_is_bot_by_historical,
    e.event_user_is_anonymous,
    e.event_user_is_temporary,
    e.event_user_is_permanent,
    e.event_user_registration_timestamp,
    e.event_user_groups_historical,
    (e.event_user_text_historical LIKE '%>%'
     AND e.event_user_is_anonymous
     AND NOT COALESCE(e.event_user_is_temporary, FALSE))               AS event_user_is_cross_wiki,

    -- No user information, page event

    e.page_id,
    e.page_title_historical,
    e.page_namespace_historical,
    e.page_namespace_is_content_historical,
    e.page_is_deleted,

    e.event_meta_id,
    e.meta_dt
  FROM page_with_bots e
)"""
    }

    def buildPageEventMergeSQL(p: Params): String =
        buildPageEventSQL(p) +
            s"""

MERGE INTO ${p.ref} t
USING page_incoming s
ON  t.wiki_id       = s.wiki_id
AND t.event_meta_id = s.event_meta_id
WHEN MATCHED AND t.source = 'events' THEN
  UPDATE SET
    t.event_entity                                                  = s.event_entity,
    t.event_type                                                    = s.event_type,
    t.event_timestamp                                               = s.event_timestamp,

    t.event_user_id                                                 = s.event_user_id,
    t.event_user_central_id                                         = s.event_user_central_id,
    t.event_user_text_historical                                    = s.event_user_text_historical,
    t.event_user_is_bot_by_historical                               = s.event_user_is_bot_by_historical,
    t.event_user_is_anonymous                                       = s.event_user_is_anonymous,
    t.event_user_is_temporary                                       = s.event_user_is_temporary,
    t.event_user_is_permanent                                       = s.event_user_is_permanent,
    t.event_user_registration_timestamp                             = s.event_user_registration_timestamp,
    -- No event_user_revision_count for page events
    t.event_user_groups_historical                                  = s.event_user_groups_historical,
    t.event_user_is_cross_wiki                                      = s.event_user_is_cross_wiki,

    t.page_id                                                       = s.page_id,
    t.page_title_historical                                         = s.page_title_historical,
    t.page_namespace_historical                                     = s.page_namespace_historical,
    t.page_namespace_is_content_historical                          = s.page_namespace_is_content_historical,
    t.page_is_deleted                                               = s.page_is_deleted,

    t.control_map = map_concat(COALESCE(t.control_map, map()),
                               map('page_meta_id',    s.event_meta_id,
                                   'page_update_dt',  date_format(s.meta_dt, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))),
    t.row_update_dt = GREATEST(t.row_update_dt, TIMESTAMP '${p.rowUpdateDt}')
WHEN NOT MATCHED THEN
  INSERT (
    source,
    wiki_id,

    event_entity,
    event_type,
    event_timestamp,

    event_user_id,
    event_user_central_id,
    event_user_text_historical,
    event_user_is_bot_by_historical,
    event_user_is_anonymous,
    event_user_is_temporary,
    event_user_is_permanent,
    event_user_registration_timestamp,
    event_user_revision_count,
    event_user_groups_historical,
    event_user_is_cross_wiki,

    user_id,
    user_central_id,
    user_text_historical,
    user_is_anonymous,
    user_is_temporary,
    user_is_permanent,
    user_groups_historical,
    user_is_bot_by_historical,
    user_is_created_by_self,
    user_is_created_by_system,
    user_is_created_by_peer,

    page_id,
    page_title_historical,
    page_namespace_historical,
    page_namespace_is_content_historical,
    page_is_deleted,

    revision_id,
    revision_parent_id,
    revision_minor_edit,
    revision_text_bytes,
    revision_text_bytes_diff,
    revision_text_sha1,
    revision_deleted_parts,
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    revision_is_deleted_by_page_deletion,
    revision_tags,

    event_meta_id,
    control_map,
    row_update_dt
  ) VALUES (
    s.source,
    s.wiki_id,

    s.event_entity,
    s.event_type,
    s.event_timestamp,

    s.event_user_id,
    s.event_user_central_id,
    s.event_user_text_historical,
    s.event_user_is_bot_by_historical,
    s.event_user_is_anonymous,
    s.event_user_is_temporary,
    s.event_user_is_permanent,
    s.event_user_registration_timestamp,
    CAST(NULL AS BIGINT),                    -- event_user_revision_count
    s.event_user_groups_historical,
    s.event_user_is_cross_wiki,

    CAST(NULL AS BIGINT),                    -- user_id
    CAST(NULL AS BIGINT),                    -- user_central_id
    CAST(NULL AS STRING),                    -- user_text_historical
    CAST(NULL AS BOOLEAN),                   -- user_is_anonymous
    CAST(NULL AS BOOLEAN),                   -- user_is_temporary
    CAST(NULL AS BOOLEAN),                   -- user_is_permanent
    CAST(NULL AS ARRAY<STRING>),             -- user_groups_historical
    CAST(NULL AS ARRAY<STRING>),             -- user_is_bot_by_historical
    CAST(NULL AS BOOLEAN),                   -- user_is_created_by_self
    CAST(NULL AS BOOLEAN),                   -- user_is_created_by_system
    CAST(NULL AS BOOLEAN),                   -- user_is_created_by_peer

    s.page_id,
    s.page_title_historical,
    s.page_namespace_historical,
    s.page_namespace_is_content_historical,
    s.page_is_deleted,

    CAST(NULL AS BIGINT),                    -- revision_id,
    CAST(NULL AS BIGINT),                    -- revision_parent_id
    CAST(NULL AS BOOLEAN),                   -- revision_minor_edit
    CAST(NULL AS BIGINT),                    -- revision_text_bytes
    CAST(NULL AS BIGINT),                    -- revision_text_bytes_diff
    CAST(NULL AS STRING),                    -- revision_text_sha1
    CAST(NULL AS ARRAY<STRING>),             -- revision_deleted_parts
    CAST(NULL AS BOOLEAN),                   -- revision_is_identity_reverted
    CAST(NULL AS BIGINT),                    -- revision_first_identity_reverting_revision_id
    CAST(NULL AS BIGINT),                    -- revision_seconds_to_identity_revert
    CAST(NULL AS BOOLEAN),                   -- revision_is_identity_revert
    CAST(NULL AS BOOLEAN),                   -- revision_is_deleted_by_page_deletion
    CAST(NULL AS ARRAY<STRING>),             -- revision_tags

    s.event_meta_id,
    map('page_meta_id',   s.event_meta_id,
        'page_update_dt', date_format(s.meta_dt, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")),
    TIMESTAMP '${p.rowUpdateDt}'             -- row_update_dt
  )"""



    /**
     * MERGE 6: back-patches page_is_deleted and revision_is_deleted_by_page_deletion on existing
     * revision rows when today's page events include a delete or undelete.
     * Joins on (wiki_id, page_id) — NOT the partition key — so this scans all monthly partitions.
     * Fires unconditionally; Spark prunes the MERGE to a no-op when the source CTE is empty.
     */
    def buildPageDeletionBackpatchMergeSQL(p: Params): String = {
        buildPageEventSQL(p) +
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
  t.control_map = map_concat(COALESCE(t.control_map, map()), map('page_deletion_dt', date_format(s.meta_dt, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))),
  t.row_update_dt = GREATEST(t.row_update_dt, TIMESTAMP '${p.rowUpdateDt}')"""
    }


}
