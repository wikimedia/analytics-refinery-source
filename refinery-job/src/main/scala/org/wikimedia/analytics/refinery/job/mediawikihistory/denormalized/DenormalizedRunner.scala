package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
  * This class defines the functions for revisions-denormalization process.
  * It delegates the revision-enrichment part of it's process to the
  * [[DenormalizedRevisionsBuilder]] object.
  *
  * The [[run]] function loads user, page and revisions data from raw path
  * and does some cleaning (see [[filterStates]] function). It then calls
  * [[DenormalizedRevisionsBuilder.run]] to enrich and join revisions data,
  * then uses the partition-and-sort-within-partition/zip-partitions trick
  * to denormalize revisions with users and pages data, and users and
  * pages data with user data.
  *
  * It finally writes the union of those denormalized data in parquet format.
  */
class DenormalizedRunner(
                          val spark: SparkSession,
                          val statsAccumulator: Option[MapAccumulator[String, Long]],
                          val numPartitions: Int
                        ) extends StatsHelper with Serializable {

  import org.apache.spark.sql.{SaveMode, Row}
  import org.apache.spark.sql.types.{StringType, StructField, StructType}
  import scala.reflect.ClassTag
  import com.databricks.spark.avro._
  import org.apache.log4j.Logger
  import org.apache.spark.rdd.RDD
  import java.sql.Timestamp
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState
  // Implicit needed to sort by timestamps
  import org.wikimedia.analytics.refinery.core.TimestampHelpers
  import TimestampHelpers.orderedTimestamp

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_INITIAL_LIVE_REVISIONS = "denormalize.initialLiveRevisions"
  val METRIC_INITIAL_ARCHIVED_REVISIONS = "denormalize.initialArchivedRevisions"

  val METRIC_INITIAL_PAGE_STATES = "denormalize.pageStates.initial"
  val METRIC_WRONG_IDS_PAGE_STATES = "denormalize.pageStates.wrongIds"
  val METRIC_FILTERED_OUT_PAGE_STATES = "denormalize.pageStates.filteredOut"
  val METRIC_INITIAL_PAGE = "denormalize.page.initial"

  val METRIC_INITIAL_USER_STATES = "denormalize.userStates.initial"
  val METRIC_FILTERED_OUT_USER_STATES = "denormalize.userStates.filteredOut"
  val METRIC_INITIAL_USER = "denormalize.user.initial"

  val METRIC_WRITTEN_ROWS = "denormalize.writtenRows"

  val workPartitions = numPartitions * 4


  /**
    * Filters states RDD by the key defined by getStateKey
    * (usually wikiDb, userId or pageId).
    *
    * Removes whole-history-states (having (start, end) = (None, None))
    * if such a state is not the only one for its key.
    *
    * @param states The states RDD
    * @param getStateKey The function providing the state key
    * @tparam S The state type
    * @return The filtered states RDD
    */
  def filterStates[S <: TimeBoundaries](
                                         states: RDD[S],
                                         getStateKey: S => StateKey,
                                         metricName: String
                                      )(implicit cTagS: ClassTag[S]): RDD[S] = {
    // TODO -- Add more complete overlapping-events filtering

    // Extract partition keys for which a whole-history-state exists
    // ((start, end) = (None, None))
    val wholeHistoryStates = states
      .flatMap(s => (s.startTimestamp, s.endTimestamp) match {
        case (None, None) => Seq((getStateKey(s).partitionKey, true))
        case _ => Seq.empty[(PartitionKey, Boolean)]
      })

    // Key join whole-history-states with all-states,
    // count number of states for those keys and keep only
    // those having more than 1 (to be removed)
    val toBeRemovedStates: RDD[(StateKey, Boolean)]= wholeHistoryStates
      .join(states.map(s => (getStateKey(s).partitionKey, true)))
      // Can't use coutByKey since it returns a Map and not a RDD
      .combineByKey(_ => 1L, (agg: Long, _) => agg + 1L, (agg1: Long, agg2: Long) => agg1 + agg2)
      .flatMap {
        case (key, count) =>
          if (count > 1L)
            Seq((new StateKey(key, None.asInstanceOf[Option[Timestamp]], None.asInstanceOf[Option[Timestamp]]), true))
          else
            Seq.empty[(StateKey, Boolean)]
      }

    // Left join all-states with to-be-filtered states
    // and keep only the non-joined ones.
    states
      .keyBy(s => getStateKey(s)) // RDD[(K, State)]
      .leftOuterJoin(toBeRemovedStates) // RDD [(K, (State, Option[_]))]
      .flatMap(t => {
        if (t._2._2.isEmpty) Seq(t._2._1)
        else {
          addOptionalStat(s"${t._1.partitionKey.db}.$metricName", 1)
          Seq.empty[S]
        }
      })
  }

  def getUserStates(userHistoryPath: String, wikiClause: String): RDD[UserState] = {
    val userStatesDf = spark.read.parquet(userHistoryPath).repartition(workPartitions).where(s"TRUE $wikiClause")
    val userStatesToFilter = userStatesDf.rdd.map(r => {
      val state = UserState.fromRow(r)
      addOptionalStat(s"${state.wikiDb}.$METRIC_INITIAL_USER_STATES", 1)
      state
    }).cache()
    filterStates[UserState](
      userStatesToFilter, DenormalizedKeysHelper.userStateKey, METRIC_FILTERED_OUT_USER_STATES
    ).cache()
  }


  def getPageStates(pageHistoryPath: String, wikiClause: String): RDD[PageState] = {
    val pageStatesDf = spark.read.parquet(pageHistoryPath).repartition(workPartitions).where(s"TRUE $wikiClause")
    val pageStatesToFilter = pageStatesDf.rdd
      .map(r => {
        val state = PageState.fromRow(r)
        addOptionalStat(s"${state.wikiDb}.$METRIC_INITIAL_PAGE_STATES", 1)
        state
      })
      .filter(state => {
        val validId = state.pageId.getOrElse(0L) > 0 && state.pageIdArtificial.isEmpty
        if (!validId) addOptionalStat(s"${state.wikiDb}.$METRIC_WRONG_IDS_PAGE_STATES", 1)
        validId
      }).cache()
    filterStates[PageState](
      pageStatesToFilter, DenormalizedKeysHelper.pageStateKey, METRIC_FILTERED_OUT_PAGE_STATES
    ).cache()
  }

  def getLiveRevisions(wikiClause: String) = {
    // TODO: simplify or remove joins as source table imports change
    // TODO: content model and format are nulled, replace with join to slots if needed
    // NOTE: rev_comment_id is created by cloud views and sanitized to 0 based on rev_deleted, it can be used to join
    // NOTE: rev_actor is created by cloud views and sanitized to null based on rev_deleted, it can be used to join
    spark.sql(
      s"""
  SELECT
    r.wiki_db AS wiki_db,
    rev_timestamp,
    coalesce(rev_comment, comment_text) AS rev_comment,
    rev_user,
    coalesce(rev_user_text, actor_name) AS rev_user_text,
    rev_page,
    rev_id,
    rev_parent_id,
    rev_minor_edit,
    rev_len,
    rev_sha1,
    null rev_content_model,
    null rev_content_format
  FROM revision r
    LEFT JOIN actor a
      ON a.actor_id = r.rev_actor
        AND a.wiki_db = r.wiki_db
    LEFT JOIN comment c
      ON c.comment_id = r.rev_comment_id
        AND c.wiki_db = r.wiki_db
  WHERE TRUE
    $wikiClause
  -- Trick to force using defined number of partitions
  GROUP BY
    r.wiki_db,
    rev_timestamp,
    coalesce(rev_comment, comment_text),
    rev_user,
    coalesce(rev_user_text, actor_name),
    rev_page,
    rev_id,
    rev_parent_id,
    rev_minor_edit,
    rev_len,
    rev_sha1
        """)
      .rdd
      .map(row => {
        val wikiDb = row.getString(0)
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_LIVE_REVISIONS", 1L)
        MediawikiEvent.fromRevisionRow(row)
      })
  }


  def getArchivedRevisionsNotFiltered(wikiClause: String): RDD[MediawikiEvent] = {
    // TODO: simplify or remove joins as source table imports change
    // TODO: content model and format are nulled, replace with join to slots if needed
    // NOTE: ar_len is nulled if ar_deleted&1, not sure how this affects metrics
    // NOTE: ar_comment is always null when it comes from cloud dbs
    // NOTE: ar_user and ar_user_text are null on cloud dbs if ar_deleted&4
    // NOTE: ar_actor is 0 on cloud dbs if ar_deleted&4
    // NOTE: ar_sha1 is null on cloud dbs if ar_deleted&1
    spark.sql(
      s"""
  SELECT
    ar.wiki_db AS wiki_db,
    ar_timestamp,
    ar_comment,
    ar_user,
    coalesce(ar_user_text, actor_name) AS ar_user_text,
    ar_page_id,
    ar_title,
    ar_namespace,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_len,
    ar_sha1,
    null AS ar_content_model,
    null AS ar_content_format
  FROM archive ar
    -- This is needed to prevent archived revisions having
    -- existing live revisions to cause problem
    FULL OUTER JOIN revision
      ON ar.wiki_db = revision.wiki_db
        AND ar.ar_rev_id = revision.rev_id
    LEFT JOIN actor a
      ON a.actor_id = ar.ar_actor
      AND a.wiki_db = ar.wiki_db
  WHERE TRUE
    AND revision.rev_id IS NULL
    ${wikiClause.replace("wiki_db", "ar.wiki_db")}
  -- Trick to force using defined number of partitions
  GROUP BY
    ar.wiki_db,
    ar_timestamp,
    ar_comment,
    ar_user,
    coalesce(ar_user_text, actor_name),
    ar_page_id,
    ar_title,
    ar_namespace,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_len,
    ar_sha1
      """)
      .rdd
      .map(row => {
        val wikiDb = row.getString(0)
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_ARCHIVED_REVISIONS", 1L)
        MediawikiEvent.fromArchiveRow(row)
      })
  }

  def filterArchivedRevisions(archivedRevisionsNotFiltered: RDD[MediawikiEvent]): RDD[MediawikiEvent] = {
    archivedRevisionsNotFiltered.
      keyBy(r => DenormalizedKeysHelper.revisionMediawikiEventKey(r).partitionKey).
      groupByKey().
      flatMap { case (k, revisionsIterator) =>
        if (k.id > 0) {
          // Keep only most recent revision if multiple
          Seq(revisionsIterator.toVector.sortBy(r => r.eventTimestamp).last)
        } else {
          // Don't touch fake Ids
          revisionsIterator.toVector
        }
      }
  }

  /**
    * Extract and clean core data needed for:
    *  - revisions data augmentation
    *  - revisions denormalization with users and pages states
    *  - users and pages denormalization with users
    *  - Union of revisions, users and pages denormalized data
    *  - result output in parquet files
    *
    * @param wikiConstraint The wiki database names on which to execute the job (empty for all wikis)
    * @param revisionDataPath The path of the revision data (avro files partitioned by wiki_db)
    * @param archiveDataPath The paths of the archive data (avro files partitioned by wiki_db)
    * @param actorDataPath The path of the actor data (avro files partitioned by wiki_db)
    * @param commentDataPath The paths of the comment data (avro files partitioned by wiki_db)
    * @param userHistoryPath The path of the user states built in UserHistory process
    * @param pageHistoryPath The path of the page states built in PageHistory process (parquet files)
    * @param outputPath The path to output the denormalized history (parquet files)
    */
  def run(
           wikiConstraint: Seq[String],
           revisionDataPath: String,
           archiveDataPath: String,
           actorDataPath: String,
           commentDataPath: String,
           userHistoryPath: String,
           pageHistoryPath: String,
           outputPath: String,
           errorsPathOption: Option[String]
  ): Unit = {

    log.info(s"Denormalized MW Events jobs starting")

    //***********************************
    // Prepare (live-archived) revisions, users and pages history RDDs
    //***********************************

    // Work with 4 times more partitions that expected for file production
    spark.sql("SET spark.sql.shuffle.partitions=" + workPartitions)

    val revisionDf = spark.read.avro(revisionDataPath)
    revisionDf.createOrReplaceTempView("revision")

    val archiveDf = spark.read.avro(archiveDataPath)
    archiveDf.createOrReplaceTempView("archive")

    val actorDf = spark.read.avro(actorDataPath)
    actorDf.createOrReplaceTempView("actor")

    val commentDf = spark.read.avro(commentDataPath)
    commentDf.createOrReplaceTempView("comment")

    val wikiClause = if (wikiConstraint.isEmpty) ""
                     else "AND wiki_db IN (" + wikiConstraint.map(w => s"'$w'").mkString(", ") + ")\n"


    val userStates = getUserStates(userHistoryPath, wikiClause)
    val pageStates = getPageStates(pageHistoryPath, wikiClause)
    val liveRevisions = getLiveRevisions(wikiClause)
    val archivedRevisionsNotFiltered = getArchivedRevisionsNotFiltered(wikiClause)


    val userMediawikiEvents = userStates.map(userState => {
      addOptionalStat(s"${userState.wikiDb}.$METRIC_INITIAL_USER", 1)
      MediawikiEvent.fromUserState(userState)
    })
    val pageMediawikiEvents = pageStates.map(pageState => {
      addOptionalStat(s"${pageState.wikiDb}.$METRIC_INITIAL_PAGE", 1)
      MediawikiEvent.fromPageState(pageState)
    })

    log.info(s"Denormalized MW Events data defined")

    //***********************************
    // Prepare for revisions updates and MW Events denormalization
    //***********************************

    // Partitioners for partition-sort-zip
    val statePartitioner = new PartitionKeyPartitioner[StateKey](workPartitions)
    val mediawikiEventPartitioner = new PartitionKeyPartitioner[MediawikiEventKey](workPartitions)

    // Partitioned-sorted user and page states for future zipping
    val sortedUserStates = userStates
      .map(userState => (DenormalizedKeysHelper.userStateKey(userState), userState))
      .repartitionAndSortWithinPartitions(statePartitioner)
      .cache()
    val sortedPageStates = pageStates
      .map(pageState => (DenormalizedKeysHelper.pageStateKey(pageState),  pageState))
      .repartitionAndSortWithinPartitions(statePartitioner)

    // user and page iterate functions setup
    val userIterate = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
      MediawikiEvent.updateWithOptionalUser(statsAccumulator)) _
    val pageIterate = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
      MediawikiEvent.updateWithOptionalPage(statsAccumulator)) _

    // User and page with previous functions setup
    val userMetricsMapperWithPrevious = DenormalizedKeysHelper.mapWithPreviouslyComputed[MediawikiEventKey, MediawikiEvent, MediawikiEvent](
      DenormalizedKeysHelper.compareMediawikiEventPartitionKeys,
      MediawikiEvent.updateWithOptionalUserPrevious(statsAccumulator)
    ) _

    val pageMetricsMapperWithPrevious = DenormalizedKeysHelper.mapWithPreviouslyComputed[MediawikiEventKey, MediawikiEvent, MediawikiEvent](
      DenormalizedKeysHelper.compareMediawikiEventPartitionKeys,
      MediawikiEvent.updateWithOptionalPagePrevious(statsAccumulator)
    ) _


    //***********************************
    // Run revision updates
    //***********************************

    // Remove duplicate revision Ids in archive, use the most recent one
    val archivedRevisions = filterArchivedRevisions(archivedRevisionsNotFiltered)

    val revisions = new DenormalizedRevisionsBuilder(spark, statsAccumulator, workPartitions).run(
      liveRevisions,
      archivedRevisions,
      pageStates,
      mediawikiEventPartitioner
    )

    log.info(s"Repartitioning and sorting denormalized revisions, adding per-user values and zipping with user states")
    val revisionsWithUserData = revisions
      .keyBy(r => DenormalizedKeysHelper.userMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(userMetricsMapperWithPrevious(it1), it2))

    log.info(s"Repartitioning and sorting denormalized revisions, adding per-page values and zipping with page states")
    val revisionsWithUserAndPageData = revisionsWithUserData
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedPageStates)((it1, it2) => pageIterate(pageMetricsMapperWithPrevious(it1), it2))


    //***********************************
    // Run user and page history updates
    //***********************************

    log.info(s"Repartitioning and sorting denormalized users, zipping with user states")
    val userMediawikiEventsWithUserData = userMediawikiEvents
      .keyBy(u => DenormalizedKeysHelper.userMediawikiEventKey(u))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))

    log.info(s"Repartitioning and sorting denormalized pages, zipping with user states")
    val pageMediawikiEventsWithUserData = pageMediawikiEvents
      .keyBy(p => DenormalizedKeysHelper.userMediawikiEventKey(p))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))

    //***********************************
    // Join revisions, user and page and write results and errors
    //***********************************

    log.info(s"Union-ing denormalized revisions, pages and users, and writing results and errors")
    val denormalizedMediawikiEventsRdd = revisionsWithUserAndPageData
      .union(userMediawikiEventsWithUserData)
      .union(pageMediawikiEventsWithUserData)

    //***********************************
    // Write results
    //***********************************

    val denormalizedMediawikiEventsDf = spark.createDataFrame(
        denormalizedMediawikiEventsRdd
          // eventErrors should not be filtered out,
          // They would impair global stats correctness
          //.filter(_.eventErrors.isEmpty)
          .map(event => {
            addOptionalStat(s"${event.wikiDb}.$METRIC_WRITTEN_ROWS", 1)
            event.toRow
          }),
        MediawikiEvent.schema)
    denormalizedMediawikiEventsDf.repartition(workPartitions).write.mode(SaveMode.Overwrite).parquet(outputPath)
    log.info(s"Denormalized MW Events results written")

    //***********************************
    // Optionally write errors
    //***********************************
    errorsPathOption.foreach(errorsPath => {
      val errorDf = spark.createDataFrame(
        denormalizedMediawikiEventsRdd
          .filter(_.eventErrors.nonEmpty)
          .map(e => Row(e.wikiDb, "zipping", e.toString)),
        StructType(Seq(
          StructField("wiki_db", StringType, nullable = false),
          StructField("error_type", StringType, nullable = false),
          StructField("event", StringType, nullable = false)
        ))
      )
      errorDf.repartition(numPartitions / 16).write.mode(SaveMode.Overwrite).format("csv").option("sep", "\t").save(errorsPath)
      log.info(s"Denormalized MW Events errors written")
    })


    log.info(s"Denormalized MW Events jobs done")

  }
}
