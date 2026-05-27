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
 * The first DELETE removes snapshot rows dropped by the monthly rebuild anywhere
 * in history (graph-lineage heuristics can mutate past events). The second DELETE
 * removes stale events rows from prior months that the snapshot now supersedes.
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
    s"""WITH projected_monthly AS (
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
    -- Authoritative tier: full-history values from wmf.mediawiki_history.
    s.revision_is_identity_reverted,
    s.revision_first_identity_reverting_revision_id,
    s.revision_seconds_to_identity_revert,
    s.revision_is_identity_revert
  FROM ${p.sourceTable} s
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
    t.revision_is_identity_reverted                                 = s.revision_is_identity_reverted,
    t.revision_first_identity_reverting_revision_id                 = s.revision_first_identity_reverting_revision_id,
    t.revision_seconds_to_identity_revert                           = s.revision_seconds_to_identity_revert,
    t.revision_is_identity_revert                                   = s.revision_is_identity_revert
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
    revision_is_identity_revert
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
    s.revision_is_identity_revert
  )
WHEN NOT MATCHED BY SOURCE AND t.source = 'snapshot' THEN
  DELETE
-- The source CTE contains only revision rows, so every source='events' revision row
-- is unconditionally "not matched by source". The upper bound is critical: the snapshot
-- for month M lands ~3 days into month M+1, meaning M+1 events rows are already in the
-- table and must not be deleted. No lower bound: stale events rows from prior months
-- (e.g. from a previously failed run) are cleaned up for free.
-- event_entity = 'revision' is required: page and user event rows are never projected
-- by this merger (no stable natural key in wmf.mediawiki_history), so they must be
-- preserved as source='events' rows and must not be deleted here.
-- TODO: extend this merger to reconcile page and user entities once a key strategy
-- is agreed (composite timestamp key vs. delete-then-insert). See T427328.
WHEN NOT MATCHED BY SOURCE
  AND t.source          = 'events'
  AND t.event_entity    = 'revision'
  AND t.event_timestamp <  TIMESTAMP '${monthEnd} 00:00:00' THEN
  DELETE"""
  }
}
