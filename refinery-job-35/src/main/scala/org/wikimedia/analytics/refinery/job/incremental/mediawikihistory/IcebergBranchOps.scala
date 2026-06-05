package org.wikimedia.analytics.refinery.job.incremental.mediawikihistory

/**
 * Shared helpers for Iceberg WAP (Write-Audit-Publish) branching.
 *
 * Used by MWHistoryDeltaWriter and MWHistorySnapshotMerger to scope all SQL
 * reads and writes to a named branch, then publish atomically via fast_forward.
 */
object IcebergBranchOps {

  /**
   * Returns the branch-scoped table reference when icebergBranch is set,
   * or the plain table name otherwise. Prepends catalog when non-empty.
   *
   * Example: targetRef("spark_catalog", "wmf.tbl", "daily_2026_06_05")
   *   => "spark_catalog.wmf.tbl.branch_daily_2026_06_05"
   */
  def targetRef(catalog: String, targetTable: String, icebergBranch: String): String = {
    val fullTable = if (catalog.nonEmpty) s"$catalog.$targetTable" else targetTable
    if (icebergBranch.nonEmpty) s"$fullTable.branch_$icebergBranch" else fullTable
  }
}
