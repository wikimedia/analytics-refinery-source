package org.wikimedia.analytics.refinery.job.mediawikihistory

import org.apache.spark.sql.SparkSession

/**
 * Knows how to split up skewed joins by the following process:
 * 1. figure out how many times to split up a skewed join
 * 2. broadcast temporary data frames that define the splits, so all workers can access it
 * 3. register udfs that return the number of splits and a list from 0 to that number
 *
 * In this way, users of this functionality can use the registered udfs to unskew their joins
 */

class SkewedJoinHelper(val spark: SparkSession) {

  def splitByWikiDbAndLong(
        table: String,
        longColumn: String,
        wikiClause: String,
        base: Int,
        thresholdMagnitude: Int) : Map[(String, Long), Int] = {

    // Trick to join skewed data.
    // Joins are skewed because many revisions share a single rev_comment_id or rev_actor.
    // We find groups of such revisions and split them up by an artificial key.
    // We then copy the corresponding rows in the actor or comment tables and join.
    // We coordinating this by broadcasting the set of (wiki_db, comment_id, splits)
    spark.sql(s"""
  SELECT
    wiki_db,
    $longColumn,
    CAST(pow($base, log10(COUNT(*)) - $thresholdMagnitude) AS INT) as splits
  FROM $table
  WHERE TRUE
    $wikiClause
  GROUP BY
    wiki_db,
    $longColumn
  HAVING count(*) > pow(10, $thresholdMagnitude + 1)
        """)
      .rdd
      .map(row => ((row.getString(0), row.getLong(1)), row.getInt(2)))
      .collect
      .toMap
  }
}
