package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Monthly snapshot merger for mediawiki_history_incremental_v1.
 *
 * Triggered after MediawikiHistoryRunner succeeds for a given snapshot. Replaces
 * the prior snapshot's rows in the Iceberg target with a fresh projection from
 * wmf.mediawiki_history via three operations:
 *
 *   1. Cleanup DELETE — removes all stale rows in one statement:
 *        source='snapshot'  (all timestamps — fully replaced by the new INSERT)
 *        source='events'    with event_timestamp < snapshot month-end
 *                           (superseded by the new snapshot; future-month events preserved)
 *
 *   2. Revision INSERT — inserts fresh source='snapshot' revision rows from
 *        wmf.mediawiki_history. Deduplicates on (wiki_id, revision_id) to guard
 *        against rare source duplicates (e.g. snapshot 2026-02).
 *
 *   3. Page/user INSERT — inserts fresh source='snapshot' page and user rows.
 *        wmf.mediawiki_history has no event_meta_id UUID for these entities, so a
 *        MERGE key is not available; delete-then-insert is used instead. Deduplicates
 *        on event_log_id (100% populated for page/user events in wmf.mediawiki_history).
 *
 * NOTE: operations 1–3 are not wrapped in a single Iceberg transaction, so there is
 * a window between DELETE and INSERT where snapshot rows are absent from the table.
 * This will be eliminated by Iceberg WAP (write-audit-publish) branching in a follow-up.
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
    val cleanupSql = buildCleanupSQL(p)
    log.info(s"Running MWHistorySnapshotMerger cleanup DELETE on ${p.targetTable}:\n$cleanupSql")
    spark.sql(cleanupSql).collect()
    val revisionSql = buildRevisionInsertSQL(p)
    log.info(s"Running MWHistorySnapshotMerger revision INSERT INTO ${p.targetTable}:\n$revisionSql")
    spark.sql(revisionSql).collect()
    val pageUserSql = buildPageUserInsertSQL(p)
    log.info(s"Running MWHistorySnapshotMerger page/user INSERT INTO ${p.targetTable}:\n$pageUserSql")
    spark.sql(pageUserSql).collect()
  }

  /**
   * Returns the DELETE statement that clears stale rows before the fresh INSERT.
   * Removes all source='snapshot' rows (replaced wholesale) and source='events' rows
   * within the snapshot window (superseded by the new snapshot data).
   * The upper bound on events rows preserves in-flight future-month events.
   */
  def buildCleanupSQL(p: Params): String = {
    val monthEnd = java.time.YearMonth.parse(p.snapshot).plusMonths(1).atDay(1)
    s"""DELETE FROM ${p.targetTable}
WHERE source = 'snapshot'
   OR (source = 'events' AND event_timestamp < TIMESTAMP '${monthEnd} 00:00:00')"""
  }

  /**
   * Returns a SELECT that projects wmf.mediawiki_history revision rows to the narrow schema.
   * Used by buildRevisionInsertSQL and exposed for tests.
   * Deduplicates on (wiki_id, revision_id) — see T427862 (snapshot 2026-02 had duplicates).
   */
  def buildRevisionSelectSQL(p: Params): String = {
    val monthEnd = java.time.YearMonth.parse(p.snapshot).plusMonths(1).atDay(1)
    s"""SELECT
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
  s.revision_is_identity_revert,
  s.event_user_is_cross_wiki,
  s.page_is_deleted,
  s.revision_is_deleted_by_page_deletion,
  CAST(NULL AS BIGINT)                                                          AS user_central_id,
  CAST(NULL AS STRING)                                                          AS event_meta_id,
  CAST(NULL AS MAP<STRING,STRING>)                                              AS control_map
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY wiki_db, revision_id
      ORDER BY event_timestamp DESC
    ) AS _rn
  FROM ${p.sourceTable}
  -- Revision rows only; page/user handled by buildPageUserInsertSQL.
  -- Upper bound excludes rows sqoop pulled from the next month.
  WHERE snapshot     = '${p.snapshot}'
    AND event_entity = 'revision'
    AND CAST(event_timestamp AS TIMESTAMP) < TIMESTAMP '${monthEnd} 00:00:00'
) s
WHERE s._rn = 1"""
  }

  def buildRevisionInsertSQL(p: Params): String =
    s"""INSERT INTO ${p.targetTable} (
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
  revision_is_identity_revert,
  event_user_is_cross_wiki,
  page_is_deleted,
  revision_is_deleted_by_page_deletion,
  user_central_id,
  event_meta_id,
  control_map
)
${buildRevisionSelectSQL(p)}"""

  /**
   * Returns an INSERT that adds fresh source='snapshot' rows for page and user entities.
   * Uses delete-then-insert semantics (no stable MERGE key in wmf.mediawiki_history for
   * these entities).
   *
   * No deduplication is applied here, unlike buildRevisionInsertSQL. Revision dedup was
   * added in response to a confirmed data-quality incident (snapshot 2026-02). For page
   * and user events, no such incident has been observed. More importantly, sampling
   * wmf.mediawiki_history shows that ~92% of page 'create' events and ~0.7% of user
   * 'create' events have NULL event_log_id (pre-logging-era records), making event_log_id
   * an unsuitable dedup key for the dominant population. Deduping on a synthetic fallback
   * key would be opaque and hard to reason about. If upstream duplicates appear for
   * page/user events they should be investigated and fixed in wmf.mediawiki_history directly.
   *
   * event_meta_id and control_map are NULL for all snapshot rows (no rerun guards needed).
   */
  def buildPageUserInsertSQL(p: Params): String = {
    val monthEnd = java.time.YearMonth.parse(p.snapshot).plusMonths(1).atDay(1)
    s"""INSERT INTO ${p.targetTable} (
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
  revision_is_identity_revert,
  event_user_is_cross_wiki,
  page_is_deleted,
  revision_is_deleted_by_page_deletion,
  user_central_id,
  event_meta_id,
  control_map
)
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
  s.revision_is_identity_revert,
  s.event_user_is_cross_wiki,
  s.page_is_deleted,
  s.revision_is_deleted_by_page_deletion,
  s.user_central_id,
  CAST(NULL AS STRING)                                                          AS event_meta_id,
  CAST(NULL AS MAP<STRING,STRING>)                                              AS control_map
FROM ${p.sourceTable} s
WHERE s.snapshot     = '${p.snapshot}'
  AND s.event_entity IN ('page', 'user')
  AND CAST(s.event_timestamp AS TIMESTAMP) < TIMESTAMP '${monthEnd} 00:00:00'"""
  }
}
