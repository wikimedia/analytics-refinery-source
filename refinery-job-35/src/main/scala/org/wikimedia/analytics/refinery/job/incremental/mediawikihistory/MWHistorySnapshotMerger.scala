package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Monthly snapshot merger for mediawiki_history_incremental_v1.
 *
 * Triggered after MediawikiHistoryRunner succeeds for a given snapshot. Projects
 * wmf.mediawiki_history (the existing 67-column Parquet table) to the narrow schema
 * and MERGE INTOs the Iceberg target with full four-clause logic:
 *   WHEN MATCHED                                          → UPDATE (snapshot rows; ON clause includes t.source = 'snapshot')
 *   WHEN NOT MATCHED                                      → INSERT
 *   WHEN NOT MATCHED BY SOURCE AND t.source = 'snapshot' → DELETE (rows dropped by monthly rebuild)
 *   WHEN NOT MATCHED BY SOURCE AND t.source = 'events'   → DELETE (events rows before snapshot month-end)
 *
 * The DELETE clause removes rows that were dropped by the monthly rebuild anywhere
 * in history (graph-lineage heuristics can mutate past events). The t.source guard
 * ensures source='events' rows are never touched.
 *
 * Bounded revert tier (_within_90_days): derived from the monthly authoritative fields.
 *   revision_is_identity_reverted_within_90_days: true when reverted and delay <= 90 days.
 *   revision_is_identity_revert_within_90_days: true when this revision IS a revert
 *     and the original was reverted within 90 days (requires a self-join on
 *     revision_first_identity_reverting_revision_id).
 *
 * One run per snapshot is sufficient: wmf.mediawiki_history always covers all of
 * history for the given snapshot month.
 */
object MWHistorySnapshotMerger {

  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  case class Params(
    sourceTable: String = "",  // wmf.mediawiki_history
    targetTable: String = "",  // mediawiki_history_incremental_v1
    snapshot: String    = ""   // YYYY-MM
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
    val parser = new OptionParser[Params]("MWHistorySnapshotMerger") {
      opt[String]("source_table").required().action((v, p) => p.copy(sourceTable = v))
      opt[String]("target_table").required().action((v, p) => p.copy(targetTable = v))
      opt[String]("snapshot").required().action((v, p) => p.copy(snapshot = v))
    }
    parser.parse(args, Params()).getOrElse(sys.exit(1))
  }

  def run(spark: SparkSession, p: Params): Unit = {
    log.info(s"MWHistorySnapshotMerger params: $p")
    val sql = buildMergeSQL(p)
    log.info(s"Running MWHistorySnapshotMerger MERGE INTO ${p.targetTable} for snapshot ${p.snapshot}:\n$sql")
    spark.sql(sql).collect()
  }

  /**
   * Returns the CTE that projects wmf.mediawiki_history to the narrow schema.
   * Tests append "SELECT * FROM projected_monthly" to verify the projection without Iceberg.
   */
  def buildProjectionSQL(p: Params): String = {
    val monthEnd = java.time.YearMonth.parse(p.snapshot).plusMonths(1).atDay(1)
    s"""WITH revert_seconds AS (
  -- For each reverting revision, look up the delay from the original to the revert.
  -- Used to derive revision_is_identity_revert_within_90_days on the reverting row.
  SELECT
    wiki_db,
    revision_first_identity_reverting_revision_id AS reverting_rev_id,
    revision_seconds_to_identity_revert
  FROM ${p.sourceTable}
  WHERE snapshot                                    = '${p.snapshot}'
    AND event_entity                                = 'revision'
    AND revision_is_identity_reverted               = true
    AND revision_first_identity_reverting_revision_id IS NOT NULL
    AND revision_seconds_to_identity_revert         IS NOT NULL
    AND CAST(event_timestamp AS TIMESTAMP)          < TIMESTAMP '${monthEnd} 00:00:00'
),

projected_monthly AS (
  -- Project the 67-column monthly Parquet table to the narrow schema.
  -- event_timestamp and event_user_registration_timestamp are stored as STRING
  -- in wmf.mediawiki_history in 'yyyy-MM-dd HH:mm:ss.S' format; cast to TIMESTAMP.
  SELECT
    'snapshot'                                                            AS source,
    s.wiki_db                                                             AS wiki_id,
    s.event_entity,
    s.event_type,
    CAST(s.event_timestamp AS TIMESTAMP)                                  AS event_timestamp,
    s.event_user_id,
    s.event_user_central_id,
    s.event_user_text_historical,
    s.event_user_is_anonymous,
    s.event_user_is_temporary,
    s.event_user_is_permanent,
    CAST(s.event_user_registration_timestamp AS TIMESTAMP)                AS event_user_registration_timestamp,
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
    s.page_namespace_is_content_historical,
    s.event_user_is_bot_by_historical,
    s.revision_deleted_parts,
    s.event_user_revision_count,
    -- Authoritative tier: full-history values from wmf.mediawiki_history.
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    -- Bounded tier: derived from authoritative fields using the 90d threshold.
    (s.revision_is_identity_reverted = true
     AND s.revision_seconds_to_identity_revert IS NOT NULL
     AND s.revision_seconds_to_identity_revert <= 90 * 86400)            AS revision_is_identity_reverted_within_90_days,
    CASE WHEN s.revision_is_identity_reverted = true
              AND s.revision_seconds_to_identity_revert IS NOT NULL
              AND s.revision_seconds_to_identity_revert <= 90 * 86400
         THEN s.revision_first_identity_reverting_revision_id
    END                                                                   AS revision_first_identity_reverting_revision_id_within_90_days,
    CASE WHEN s.revision_is_identity_reverted = true
              AND s.revision_seconds_to_identity_revert IS NOT NULL
              AND s.revision_seconds_to_identity_revert <= 90 * 86400
         THEN s.revision_seconds_to_identity_revert
    END                                                                   AS revision_seconds_to_identity_revert_within_90_days,
    (s.revision_is_identity_revert = true
     AND rs.revision_seconds_to_identity_revert IS NOT NULL
     AND rs.revision_seconds_to_identity_revert <= 90 * 86400)           AS revision_is_identity_revert_within_90_days
  FROM ${p.sourceTable} s
  LEFT JOIN revert_seconds rs
    ON  s.wiki_db      = rs.wiki_db
    AND s.revision_id  = rs.reverting_rev_id
  -- Revision rows only. Page and user entities lack a stable natural key for MERGE;
  -- extend this filter when those entity types are wired up.
  -- wmf.mediawiki_history is rebuilt from sqoop dumps that can include rows beyond the
  -- snapshot month-end (the sqoop window overlaps into the next month). Without the upper
  -- bound those rows would be inserted as source='snapshot' and create duplicates alongside
  -- source='events' rows for the same revision_id.
  WHERE s.snapshot     = '${p.snapshot}'
    AND s.event_entity = 'revision'
    AND CAST(s.event_timestamp AS TIMESTAMP) < TIMESTAMP '${monthEnd} 00:00:00'
)"""
  }

  def buildMergeSQL(p: Params): String = {
    val monthEnd = java.time.YearMonth.parse(p.snapshot).plusMonths(1).atDay(1)

    buildProjectionSQL(p) +
    s"""

MERGE INTO ${p.targetTable} t
USING projected_monthly s
ON  t.source       = 'snapshot'
AND t.wiki_id      = s.wiki_id
AND t.event_entity = 'revision'
AND t.revision_id  = s.revision_id
WHEN MATCHED THEN
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
    t.page_namespace_is_content_historical                          = s.page_namespace_is_content_historical,
    t.event_user_is_bot_by_historical                               = s.event_user_is_bot_by_historical,
    t.revision_deleted_parts                                        = s.revision_deleted_parts,
    t.event_user_revision_count                                     = s.event_user_revision_count,
    t.revision_is_identity_reverted                                 = s.revision_is_identity_reverted,
    t.revision_first_identity_reverting_revision_id                 = s.revision_first_identity_reverting_revision_id,
    t.revision_seconds_to_identity_revert                           = s.revision_seconds_to_identity_revert,
    t.revision_is_identity_revert                                   = s.revision_is_identity_revert,
    t.revision_is_identity_reverted_within_90_days                  = s.revision_is_identity_reverted_within_90_days,
    t.revision_first_identity_reverting_revision_id_within_90_days  = s.revision_first_identity_reverting_revision_id_within_90_days,
    t.revision_seconds_to_identity_revert_within_90_days            = s.revision_seconds_to_identity_revert_within_90_days,
    t.revision_is_identity_revert_within_90_days                    = s.revision_is_identity_revert_within_90_days
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
    page_namespace_is_content_historical,
    event_user_is_bot_by_historical,
    revision_deleted_parts,
    event_user_revision_count,
    revision_is_identity_reverted,
    revision_first_identity_reverting_revision_id,
    revision_seconds_to_identity_revert,
    revision_is_identity_revert,
    revision_is_identity_reverted_within_90_days,
    revision_first_identity_reverting_revision_id_within_90_days,
    revision_seconds_to_identity_revert_within_90_days,
    revision_is_identity_revert_within_90_days
  ) VALUES (
    s.source,
    s.wiki_id,
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
    s.page_namespace_is_content_historical,
    s.event_user_is_bot_by_historical,
    s.revision_deleted_parts,
    s.event_user_revision_count,
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert,
    s.revision_is_identity_reverted_within_90_days,
    s.revision_first_identity_reverting_revision_id_within_90_days,
    s.revision_seconds_to_identity_revert_within_90_days,
    s.revision_is_identity_revert_within_90_days
  )
WHEN NOT MATCHED BY SOURCE AND t.source = 'snapshot' THEN
  DELETE
-- The source CTE contains only source='snapshot' rows, so every source='events' row
-- is unconditionally "not matched by source". The upper bound is critical: the snapshot
-- for month M lands ~3 days into month M+1, meaning M+1 events rows are already in the
-- table and must not be deleted. No lower bound: stale events rows from prior months
-- (e.g. from a previously failed run) are cleaned up for free.
WHEN NOT MATCHED BY SOURCE
  AND t.source          = 'events'
  AND t.event_timestamp <  TIMESTAMP '${monthEnd} 00:00:00' THEN
  DELETE"""
  }
}
