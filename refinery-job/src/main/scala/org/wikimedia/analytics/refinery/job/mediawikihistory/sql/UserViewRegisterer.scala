package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

import scala.collection.mutable

/**
 * This class provides spark-sql-view registration for user
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     com.databricks.spark.avro, parquet, json, csv
 */
class UserViewRegisterer(
  val spark: SparkSession,
  val statsAccumulator: Option[MapAccumulator[String, Long]],
  val numPartitions: Int,
  val wikiClause: String,
  val readerFormat: String
) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  // View names for not reusable views
  private val userUnprocessedView = "user_unprocessed"
  private val userGroupsUnprocessedView = "user_groups_unprocessed"

  /**
   * Register the user view in spark session joining the user unprocessed table
   * to the revision and archive tables for first-revision timestamp
   */
  def registerUserView(
    userUnprocessedPath : String,
    userGroupsUnprocessedPath : String
  ): Unit = {

    log.info(s"Registering user view")

    // Register needed unprocessed-views
    spark.read.format(readerFormat).load(userUnprocessedPath).createOrReplaceTempView(userUnprocessedView)
    spark.read.format(readerFormat).load(userGroupsUnprocessedPath).createOrReplaceTempView(userGroupsUnprocessedView)

    // Register needed UDF
    SQLHelper.registerDedupListUDF(spark)

    // Assert that needed revision view is already registered
    assert(spark.sqlContext.tableNames().contains(SQLHelper.REVISION_VIEW))
    assert(spark.sqlContext.tableNames().contains(SQLHelper.ARCHIVE_VIEW))

    spark.sql(s"""

WITH filtered_user AS (
  SELECT
    wiki_db,
    user_id,
    user_name AS user_text,
    user_registration
  FROM $userUnprocessedView
  WHERE TRUE
    $wikiClause
    -- Drop wrong user_id
    AND user_id IS NOT NULL
    AND user_id > 0
    -- Drop wrong user_name
    AND user_name IS NOT NULL
),

-- We want both live and archive revision to get best-approximation of user-creation date
-- if it's not defined in the user table
user_first_live_revision AS (
  SELECT
    -- TODO: this is null if the user is anonymous but also null if rev_deleted&4, need to verify that's ok
    wiki_db,
    user_id,
    user_first_rev_id,
    user_first_rev_timestamp
  FROM (
    SELECT
      wiki_db,
      rev_user AS user_id,
      rev_id AS user_first_rev_id,
      rev_timestamp AS user_first_rev_timestamp,
      row_number() OVER (PARTITION BY wiki_db, rev_user ORDER BY rev_timestamp, rev_id) as row_num
    FROM ${SQLHelper.REVISION_VIEW}
    WHERE TRUE
      -- Drop undefined rev_user (not done in view)
      AND rev_user IS NOT NULL
      AND rev_user > 0
  ) t
  WHERE row_num = 1

),

user_first_archive_revision AS (
  SELECT
    wiki_db,
    user_id,
    user_first_rev_id,
    user_first_rev_timestamp
  FROM (
    SELECT
      wiki_db,
      ar_user AS user_id,
      ar_rev_id AS user_first_rev_id,
      ar_timestamp AS user_first_rev_timestamp,
      row_number() OVER (PARTITION BY wiki_db, ar_user ORDER BY ar_timestamp, ar_rev_id) as row_num
    FROM ${SQLHelper.ARCHIVE_VIEW}
    WHERE TRUE
      -- Drop undefined rev_user (not done in view)
      AND ar_user IS NOT NULL
      AND ar_user > 0
  ) t
  WHERE row_num = 1

),

user_first_revision AS (
  SELECT
    wiki_db,
    user_id,
    user_first_rev_timestamp
  FROM (
    SELECT
      wiki_db,
      user_id,
      user_first_rev_timestamp,
      row_number() OVER (PARTITION BY wiki_db, user_id ORDER BY user_first_rev_timestamp, user_first_rev_id) as row_num
    FROM (
      SELECT * FROM user_first_live_revision
      UNION ALL
      SELECT * FROM user_first_archive_revision
    ) u
  ) t
  WHERE row_num = 1
),

grouped_user_groups AS (
SELECT
  wiki_db,
  ug_user,
  dedup_list(collect_list(ug_group)) as user_groups
FROM $userGroupsUnprocessedView
  WHERE TRUE
    $wikiClause
GROUP BY
  wiki_db,
  ug_user
)

SELECT
  u.wiki_db,
  u.user_id,
  u.user_text,
  u.user_registration,
  fr.user_first_rev_timestamp,
  ug.user_groups
FROM filtered_user u
  LEFT JOIN user_first_revision fr
    ON u.user_id = fr.user_id
    AND u.wiki_db = fr.wiki_db
  LEFT JOIN grouped_user_groups ug
    ON u.wiki_db = ug.wiki_db
    AND u.user_id = ug.ug_user
GROUP BY
  -- Grouping by to enforce expected partitioning
  u.wiki_db,
  u.user_id,
  u.user_text,
  u.user_registration,
  fr.user_first_rev_timestamp,
  ug.user_groups
    """
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.USER_VIEW)

    log.info(s"User view registered")

  }

}
