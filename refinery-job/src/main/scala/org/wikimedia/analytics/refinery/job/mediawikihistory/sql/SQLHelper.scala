package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Knows how to split up skewed joins by the following process:
 * 1. figure out how many times to split up a skewed join
 * 2. broadcast temporary data frames that define the splits, so all workers can access it
 * 3. register udfs that return the number of splits and a list from 0 to that number
 *
 * In this way, users of this functionality can use the registered udfs to unskew their joins
 */

object SQLHelper {

  val NAMESPACES_VIEW = "project_namespace_map"

  val ARCHIVE_VIEW = "archive"
  val CHANGE_TAGS_VIEW = "change_tags"
  val LOGGING_VIEW = "logging"
  val PAGE_VIEW = "page"
  val REVISION_VIEW = "revision"
  val USER_VIEW = "user"

  /**
   * Function building a clause to be included in a WHERE statement.
   * @param column The column to filter on
   * @param values The values the column can be (no filtering if values is empty)
   * @param quote The quoting char to use (defaults to ', should be set to emtpy-string for integer values)
   * @return The IN clause
   */
  def inClause(column: String, values: Seq[String], quote: String = "'"): String = {
    if (values.isEmpty)
      ""
    else {
      val quotedValues = values.map(w => s"$quote$w$quote")
      s"AND $column IN (${quotedValues.mkString(", ")})\n"
    }
  }

  def skewSplits(
        table: String,
        columns: String,
        whereClauses: String,
        powBase: Int,
        log10ThresholdMagnitude: Int
  ) : String = {
    // Trick to join skewed data.
    // Joins are skewed because many revisions share a single rev_comment_id or rev_actor.
    // We find groups of such revisions and split them up by an artificial key.
    // We then copy the corresponding rows in the actor or comment tables and join.
    // We coordinating this by broadcasting the set of (wiki_db, comment_id, splits)
    s"""
  SELECT
    $columns,
    CAST(pow($powBase, log10(COUNT(*)) - $log10ThresholdMagnitude) AS INT) as splits
  FROM $table
  WHERE TRUE
    $whereClauses
  GROUP BY
    $columns
  HAVING count(*) > pow(10, $log10ThresholdMagnitude + 1)
        """
  }

  /**
   * Function registering the dedup_list UDF to the given spark-context
   */
  def registerDedupListUDF(spark: SparkSession): Unit = {
    // UDF deduplicating elements in a list (returns distinct elements)
    // Needed to deduplicate user groups (in case)
    spark.udf.register(
      "dedup_list",
      (a1: mutable.WrappedArray[String]) => a1.distinct
    )
  }

}
