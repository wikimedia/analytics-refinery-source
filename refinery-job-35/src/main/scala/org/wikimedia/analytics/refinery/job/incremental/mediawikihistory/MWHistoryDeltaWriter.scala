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
 *   MERGE 3 (buildTagsMergeSQL):          revision_tags
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

    // Airflow data_interval_end for this daily run = logical_date (year/month/day) + 1 day at
    // UTC midnight. Stamped into row_update_dt at every write site so downstream consumers can
    // read incrementally via `WHERE row_update_dt >= watermark`. Derived from the partition day
    // (single source of truth), so it is deterministic and rerun-stable. Requires the session
    // to run in UTC (the TIMESTAMP literal is interpreted in spark.sql.session.timeZone).
    def rowUpdateDt: String =
      java.time.LocalDate.of(year, month, day).plusDays(1).atStartOfDay()
        .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
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
      // Output parameters
      opt[String]("catalog").action((v, p) => p.copy(catalog = v))
      opt[String]("target_table").required().action((v, p) => p.copy(targetTable = v))
      opt[String]("iceberg_branch").action((v, p) => p.copy(icebergBranch = v))
      // Input parameters
      opt[String]("page_change_table").required().action((v, p) => p.copy(pageChangeTable = v))
      opt[String]("tags_table").required().action((v, p) => p.copy(tagsTable = v))
      opt[String]("visibility_table").required().action((v, p) => p.copy(visibilityTable = v))
      opt[String]("user_change_table").required().action((v, p) => p.copy(userChangeTable = v))
      opt[String]("namespaces_table").required().action((v, p) => p.copy(namespacesTable = v))
      opt[String]("namespaces_snapshot").required().action((v, p) => p.copy(namespacesSnapshot = v))
      opt[Int]("year").required().action((v, p) => p.copy(year = v))
      opt[Int]("month").required().action((v, p) => p.copy(month = v))
      opt[Int]("day").required().action((v, p) => p.copy(day = v))
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
    val mergeSql = MWHistoryDeltaRevisionSQL.buildRevisionEventMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter MERGE INTO ${p.ref}:\n$mergeSql")
    spark.sql(mergeSql).collect()
    // MERGE 2: back-patch seed rows whose first reverter arrived today.
    val backPatchSql = MWHistoryDeltaRevisionSQL.buildRevertedRevisionBackpatchMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter back-patch MERGE INTO ${p.ref}:\n$backPatchSql")
    spark.sql(backPatchSql).collect()
    // MERGE 3: update revision_tags from today's revision_tags_change events.
    // Separate stream from page_change_v1; tags are applied asynchronously after revision creation.
    val tagsSql = MWHistoryDeltaRevisionSQL.buildRevisionTagsMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter tags MERGE INTO ${p.ref}:\n$tagsSql")
    spark.sql(tagsSql).collect()
    // MERGE 4: update revision_deleted_parts from today's revision_visibility_change events.
    // Visibility changes are applied independently of edits and can affect revisions of any age.
    // The source (~2,700 rows/day) broadcasts automatically; no partition-pruning bound needed.
    val visibilitySql = MWHistoryDeltaRevisionSQL.buildRevisionVisibilityMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter visibility MERGE INTO ${p.ref}:\n$visibilitySql")
    spark.sql(visibilitySql).collect()
    // MERGE 5: insert/update today's page events (move, delete, undelete).
    // Keyed on (wiki_id, event_meta_id) where event_meta_id = meta.id. revision_id is NULL
    // for all page events (matches wmf.mediawiki_history convention).
    val pageEventSql = MWHistoryDeltaPageSQL.buildPageEventMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter page events MERGE INTO ${p.ref}:\n$pageEventSql")
    spark.sql(pageEventSql).collect()
    // MERGE 6: back-patch page_is_deleted and revision_is_deleted_by_page_deletion on existing
    // revision rows when today's page events include a delete or undelete. Runs after M5 so
    // the page_deletion state from today is committed before the revision back-patch reads it.
    val pageDeletionBackpatchSql = MWHistoryDeltaPageSQL.buildPageDeletionBackpatchMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter page deletion back-patch MERGE INTO ${p.ref}:\n$pageDeletionBackpatchSql")
    spark.sql(pageDeletionBackpatchSql).collect()
    // MERGE 7: insert/update today's user events (create, rename, groups_change).
    // Keyed on (wiki_id, event_meta_id) where event_meta_id = meta.id. revision_id is NULL
    // for all user events (matches wmf.mediawiki_history convention).
    val userEventSql = MWHistoryDeltaUserSQL.buildUserEventMergeSQL(p)
    log.info(s"Running MWHistoryDeltaWriter user events MERGE INTO ${p.ref}:\n$userEventSql")
    spark.sql(userEventSql).collect()
    // MERGE 7b: back-fill user_is_created_by_* on today's rename/altergroups rows.
    // The stream carries no creation-provenance for non-create events; this joins the
    // newly-inserted rows to the corresponding create-event row already in the target table.
    val userCreationProvenanceSql = MWHistoryDeltaUserSQL.buildUserCreationProvenanceBackfillMergeSQL(p)
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

}
