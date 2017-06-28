package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import org.apache.spark.sql.SQLContext

/**
  * This class defines the functions for the user history reconstruction process.
  * It delegates the reconstruction part of it's process to the
  * [[UserHistoryBuilder]] class.
  *
  * The [[run]] function loads [[UserEvent]] and [[UserState]] RDDs from raw path
  * using [[UserEventBuilder]] utilities. It then calls
  * [[UserHistoryBuilder.run]] to partition the RDDs and rebuild history.
  *
  * It finally writes the resulting [[UserState]] data in parquet format.
  *
  * Note: You can have errors output as well by providing
  * errorsPath to the [[run]] function.
  */
class UserHistoryRunner(sqlContext: SQLContext) extends Serializable {

  import org.apache.spark.sql.SaveMode
  import com.databricks.spark.avro._
  import org.apache.log4j.Logger
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)


  /**
    * Extract and clean [[UserEvent]] and [[UserState]] RDDs,
    * then launch the reconstruction and
    * writes the results (and potentially the errors).
    *
    * @param wikiConstraint The wiki database names on which to execute the job (empty for all wikis)
    * @param loggingDataPath The path of the logging data (avro files partitioned by wiki_db)
    * @param userDataPath The path of the user data (avro files partitioned by wiki_db)
    * @param userGroupsDataPath The path of the user_groups data (avro files partitioned by wiki_db)
    * @param revisionDataPath The path of the revision data (avro files partitioned by wiki_db)
    * @param outputPath The path to output the reconstructed user history (parquet files)
    * @param sqlPartitions The number of partitions to use as a bases for raw RDDs
    * @param errorsPath An optional path to output errors if defined (csv files)
    */
  def run(
           wikiConstraint: Seq[String],
           loggingDataPath: String,
           userDataPath: String,
           userGroupsDataPath: String,
           revisionDataPath: String,
           outputPath: String,
           sqlPartitions: Int,
           errorsPath: Option[String] = None
  ): Unit = {

    log.info(s"User history jobs starting")


    //***********************************
    // Prepare user events and states RDDs
    //***********************************

    sqlContext.sql("SET spark.sql.shuffle.partitions=" + sqlPartitions)

    val loggingDf = sqlContext.read.avro(loggingDataPath)
    loggingDf.registerTempTable("logging")

    val userDf = sqlContext.read.avro(userDataPath)
    userDf.registerTempTable("user")

    val userGroupsDf = sqlContext.read.avro(userGroupsDataPath)
    userGroupsDf.registerTempTable("user_groups")

    val revisionDf = sqlContext.read.avro(revisionDataPath)
    revisionDf.registerTempTable("revision")

    val wikiClause = if (wikiConstraint.isEmpty) "" else {
      "AND wiki_db IN (" + wikiConstraint.map(w => s"'$w'").mkString(", ") + ")\n"
    }

    val parsedUserEvents = sqlContext.sql(
      s"""
  SELECT
    log_type,
    log_action,
    log_timestamp,
    log_user,
    log_title,
    log_comment,
    log_params,
    wiki_db
  FROM logging
  WHERE
    log_type IN (
      'renameuser',
      'rights',
      'block',
      'newusers'
    )
    $wikiClause
  GROUP BY -- Grouping by to enforce expected partitioning
    log_type,
    log_action,
    log_timestamp,
    log_user,
    log_title,
    log_comment,
    log_params,
    wiki_db
        """)
      .rdd
      .filter(UserEventBuilder.isValidLog)
      .map(UserEventBuilder.buildUserEvent)

    val userEvents = parsedUserEvents.filter(_.parsingErrors.isEmpty).cache()



    /** *********************************************************
      * HACK
      *   - collect_set function is only available in spark 1.6 using HiveContext
      *   - HiveContext doesn't work when using Spark with Oozie
      *   --> Reimplementing the portion of code without collect_set
      *   --> Update coalesce to in map null change (typing issue)
      */

    //sqlContext.udf.register("emptyStringArray", () => Array.empty[String])

    val userGroupsSchema = StructType(
      Seq(StructField("wiki_db", StringType, nullable = false),
        StructField("ug_user", LongType, nullable = false),
        StructField("user_groups_latest", ArrayType(StringType, containsNull = false), nullable = false)))

    val userGroupsRdd = sqlContext.sql(
      s"""
    SELECT
      wiki_db,
      ug_user,
      ug_group
    FROM user_groups
      WHERE TRUE
      $wikiClause
        """)
      .rdd
      .map(r => ((r.getString(0), r.getLong(1)), Seq(r.getString(2))))
      .reduceByKey(_ ++ _)
      .map(t => Row(t._1._1, t._1._2, t._2.distinct))

      sqlContext.createDataFrame(userGroupsRdd, userGroupsSchema).registerTempTable("grouped_user_groups")


    val userStates = sqlContext.sql(
      s"""
  SELECT
    user_id,
    user_name,
    user_registration,
    u.wiki_db,
    rev.rev_timestamp as first_rev_timestamp,
    --coalesce(user_groups_latest, emptyStringArray())
    user_groups_latest
  FROM user AS u
    LEFT JOIN (
      SELECT
        rev_user,
        min(rev_timestamp) as rev_timestamp,
        wiki_db
      FROM revision
      WHERE TRUE
        $wikiClause
      GROUP BY
        rev_user,
        wiki_db
    ) rev
    ON user_id = rev_user
    AND u.wiki_db = rev.wiki_db
    LEFT JOIN grouped_user_groups ug
      -- HACK CONTINUATION
      --(
      --SELECT
      --    wiki_db,
      --    ug_user,
      --    collect_set(ug_group) as user_groups_latest
      --FROM user_groups
      --WHERE TRUE
      --  $wikiClause
      --GROUP BY
      --  wiki_db,
      --  ug_user
      --) ug
    ON u.wiki_db = ug.wiki_db
    AND user_id = ug_user
  WHERE user_id IS NOT NULL
    AND user_name IS NOT NULL -- to prevent any NPE when graph partitioning
    ${wikiClause.replace("wiki_db", "u.wiki_db")}
  GROUP BY -- Grouping by to enforce expected partitioning
    user_id,
    user_name,
    user_registration,
    u.wiki_db,
    rev.rev_timestamp,
    --coalesce(user_groups_latest, emptyStringArray())
    user_groups_latest
        """)
      .rdd
      .map { row =>
        new UserState(
            userId = row.getLong(0),
            userName = row.getString(1),
            userNameLatest = row.getString(1),
            userRegistrationTimestamp = (row.getString(2), row.getString(4)) match {
              case (null, null) => None
              case (null, potentialTimestamp) => TimestampHelpers.makeMediawikiTimestamp(potentialTimestamp)
              case (potentialTimestamp, _) => TimestampHelpers.makeMediawikiTimestamp(potentialTimestamp)
            },
            userGroups = Seq.empty[String],
            userGroupsLatest = if (row.isNullAt(5)) Seq.empty[String] else row.getSeq(5),
            userBlocks = Seq.empty[String],
            causedByEventType = "create",
            wikiDb = row.getString(3)
        )}
      .cache()


    /** *********************************************************
      * END OF HACK
      */

    log.info(s"User history data defined, starting reconstruction")


    //***********************************
    // Reconstruct user history
    //***********************************

    val userHistoryBuilder = new UserHistoryBuilder(sqlContext)
    val (userHistoryRdd, unmatchedEvents) = userHistoryBuilder.run(userEvents, userStates, errorsPath.isDefined)

    // TODO : Compute is_bot_for_other_wikis

    log.info(s"User history reconstruction done, writing results (and errors if specified)")


    //***********************************
    // Write results (and possibly errors)
    //***********************************



    val userHistoryDf =
      sqlContext.createDataFrame(userHistoryRdd.map(_.toRow), UserState.schema)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    userHistoryDf.write.mode(SaveMode.Overwrite).parquet(outputPath)
    log.info(s"User history reconstruction results written")

    val parsingErrorEvents = parsedUserEvents.filter(_.parsingErrors.nonEmpty).cache()
    log.info("User history parsing errors: " + parsingErrorEvents.count.toString)
    if (errorsPath.isDefined) {
      val matchingErrorEvents = unmatchedEvents.right.get
      log.info("Unmatched userEvents: " + matchingErrorEvents.count.toString)
      val errorDf = sqlContext.createDataFrame(
        parsingErrorEvents.map(e => Row("parsing", e.toString)).union(
          matchingErrorEvents.map(e => Row("matching", e.toString))
        ),
        StructType(Seq(
          StructField("type", StringType, nullable = false),
          StructField("event", StringType, nullable = false)
        ))
      )
      errorDf.write.mode(SaveMode.Overwrite).format("csv").option("sep", "\t").save(errorsPath.get)
      log.info(s"User history reconstruction errors written")
    } else {
      log.info("Unmatched user history events: " + unmatchedEvents.left.get.toString)
    }

    log.info(s"User history jobs done")
  }

}
