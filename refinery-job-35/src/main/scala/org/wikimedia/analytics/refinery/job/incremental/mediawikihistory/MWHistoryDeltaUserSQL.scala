package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.wikimedia.analytics.refinery.job.incremental.mediawikihistory.MWHistoryDeltaWriter.Params

object MWHistoryDeltaUserSQL {

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
     *     All three are NULL for non-create events; back-filled by MERGE 7b for rename/altergroups.
     * (4) groups_change mapped to 'altergroups' to match wmf.mediawiki_history vocabulary.
     */
    def buildUserEventSQL(p: Params): String =
        s"""WITH

raw_user_events AS (
  SELECT
    wiki_id,
    user_change_kind,
    to_timestamp(dt)               AS event_timestamp,
    meta.id                        AS event_meta_id,
    to_timestamp(meta.dt)          AS meta_dt,

    user.user_id                   AS user_id,
    user.user_central_id           AS user_central_id,
    user.user_text                 AS user_text_historical,
    is_autocreate                  AS user_is_autocreate,
    user.is_temp                   AS user_is_temp,
    user.edit_count                AS user_revision_edit_count,
    user.groups                    AS user_groups_historical,
    user.registration_dt           AS user_registration_dt,

    performer.user_id              AS event_user_id,
    performer.user_central_id      AS event_user_central_id,
    performer.user_text            AS event_user_text_historical,
    performer.is_temp              AS event_user_is_temp,
    performer.registration_dt      AS event_user_registration_dt,
    performer.groups               AS event_user_groups_historical
  FROM ${p.userChangeTable}
  WHERE year  = ${p.year}
    AND month = ${p.month}
    AND day   = ${p.day}
    AND user_change_kind IN ('create', 'rename', 'groups_change')
),

deduplicated_user AS (
  -- Deduplicate on meta.id (the MERGE 7 join key). meta.id is a delivery UUID with no
  -- repeats within a single day; this guard handles partition re-reads on reruns.
  SELECT * FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY wiki_id, event_meta_id
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
        CASE WHEN lower(e.event_user_text_historical) RLIKE '(?i)^.*bot([^a-z].*$$|$$)'
             THEN 'name' END,
        CASE WHEN array_contains(COALESCE(e.event_user_groups_historical, array()), 'bot')
             THEN 'group' END
      ),
      x -> x IS NOT NULL
    ) AS event_user_is_bot_by_historical,
    filter(
      array(
        CASE WHEN lower(e.user_text_historical) RLIKE '(?i)^.*bot([^a-z].*$$|$$)'
             THEN 'name' END,
        CASE WHEN array_contains(COALESCE(e.user_groups_historical, array()), 'bot')
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

    'user'                                                              AS event_entity,
    CASE e.user_change_kind
      WHEN 'create'        THEN 'create'
      WHEN 'rename'        THEN 'rename'
      WHEN 'groups_change' THEN 'altergroups'
    END                                                                 AS event_type,
    e.event_timestamp,

    -- event_user_* = performer (the admin/system performing the change)
    e.event_user_id,
    e.event_user_central_id,
    e.event_user_text_historical,
    e.event_user_is_bot_by_historical,
    (e.event_user_id IS NULL)                                           AS event_user_is_anonymous,
    COALESCE(e.event_user_is_temp, FALSE)                               AS event_user_is_temporary,
    (e.event_user_id IS NOT NULL
     AND NOT COALESCE(e.event_user_is_temp, FALSE))                     AS event_user_is_permanent,
    to_timestamp(e.event_user_registration_dt)                          AS event_user_registration_timestamp,
    e.event_user_groups_historical,

    -- user_* = the user being created/renamed/altered (the entity)
    e.user_id,
    e.user_central_id,
    e.user_text_historical,
    (e.user_id IS NULL)                                                 AS user_is_anonymous,
    COALESCE(e.user_is_temp, FALSE)                                     AS user_is_temporary,
    (e.user_id IS NOT NULL
     AND NOT COALESCE(e.user_is_temp, FALSE))                           AS user_is_permanent,
    e.user_groups_historical,
    e.user_is_bot_by_historical,
    CASE WHEN e.user_change_kind = 'create'
         THEN (NOT COALESCE(e.user_is_autocreate, FALSE)
               AND COALESCE(e.event_user_id = e.user_id, FALSE))
         ELSE NULL
    END                                                                 AS user_is_created_by_self,
    CASE WHEN e.user_change_kind = 'create'
         THEN COALESCE(e.user_is_autocreate, FALSE)
         ELSE NULL
    END                                                                 AS user_is_created_by_system,
    CASE WHEN e.user_change_kind = 'create'
         THEN (NOT COALESCE(e.user_is_autocreate, FALSE)
               AND NOT COALESCE(e.event_user_id = e.user_id, FALSE))
         ELSE NULL
    END                                                                 AS user_is_created_by_peer,
    (e.event_user_text_historical LIKE '%>%'
     AND (e.event_user_id IS NULL)
     AND NOT COALESCE(e.event_user_is_temp, FALSE))                     AS event_user_is_cross_wiki,

    e.event_meta_id,
    e.meta_dt
  FROM user_with_bots e
)"""

    def buildUserEventMergeSQL(p: Params): String =
        buildUserEventSQL(p) +
            s"""

MERGE INTO ${p.ref} t
USING user_incoming s
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
    -- No event_user_revision_count for user events
    t.event_user_groups_historical                                  = s.event_user_groups_historical,
    t.event_user_is_cross_wiki                                      = s.event_user_is_cross_wiki,

    t.user_id                                                       = s.user_id,
    t.user_central_id                                               = s.user_central_id,
    t.user_text_historical                                          = s.user_text_historical,
    t.user_is_anonymous                                             = s.user_is_anonymous,
    t.user_is_temporary                                             = s.user_is_temporary,
    t.user_is_permanent                                             = s.user_is_permanent,
    t.user_groups_historical                                        = s.user_groups_historical,
    t.user_is_bot_by_historical                                     = s.user_is_bot_by_historical,
    t.user_is_created_by_self                                       = s.user_is_created_by_self,
    t.user_is_created_by_system                                     = s.user_is_created_by_system,
    t.user_is_created_by_peer                                       = s.user_is_created_by_peer,

    t.control_map = map_concat(COALESCE(t.control_map, map()),
                               map('user_meta_id',   s.event_meta_id,
                                   'user_update_dt', date_format(s.meta_dt, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))),
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

    s.user_id,
    s.user_central_id,
    s.user_text_historical,
    s.user_is_anonymous,
    s.user_is_temporary,
    s.user_is_permanent,
    s.user_groups_historical,
    s.user_is_bot_by_historical,
    s.user_is_created_by_self,
    s.user_is_created_by_system,
    s.user_is_created_by_peer,

    CAST(NULL AS BIGINT),                    -- page_id
    CAST(NULL AS STRING),                    -- page_title_historical
    CAST(NULL AS INT),                       -- page_namespace_historical
    CAST(NULL AS BOOLEAN),                   -- page_namespace_is_content_historical
    CAST(NULL AS BOOLEAN),                   -- page_is_deleted

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
    map('user_meta_id',   s.event_meta_id,
        'user_update_dt', date_format(s.meta_dt, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")),
    TIMESTAMP '${p.rowUpdateDt}'             -- row_update_dt
  )"""

    /**
     * Returns SQL that back-fills user_is_created_by_* on rename/altergroups rows inserted
     * by MERGE 7, by joining them to the corresponding create-event row already in the target
     * table. Runs as a separate MERGE so it can read the target after MERGE 7 has committed.
     */
    def buildUserCreationProvenanceBackfillMergeSQL(p: Params): String =
        buildUserEventSQL(p) +
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
