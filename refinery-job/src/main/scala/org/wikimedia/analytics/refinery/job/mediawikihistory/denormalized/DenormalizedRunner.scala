package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.SQLContext

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
class DenormalizedRunner(sqlContext: SQLContext) extends Serializable {

  import org.apache.spark.sql.SaveMode
  import scala.reflect.ClassTag
  import com.databricks.spark.avro._
  import org.apache.log4j.Logger
  import org.apache.spark.rdd.RDD
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState
  import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.MediawikiEvent
  import java.sql.Timestamp
  // Implicit needed to sort by timestamps
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers.orderedTimestamp

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)


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
                                         getStateKey: S => StateKey
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
      .keyBy(s => getStateKey(s))
      .leftOuterJoin(toBeRemovedStates)
      .filter(_._2._2.isEmpty)
      .map(_._2._1)
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
    * @param userHistoryPath The path of the user states built in UserHistory process
    * @param pageHistoryPath The path of the page states built in PageHistory process (parquet files)
    * @param outputPath The path to output the denormalized history (parquet files)
    * @param sqlPartitions The number of partitions to use as a bases for raw RDDs
    */
  def run(
           wikiConstraint: Seq[String],
           revisionDataPath: String,
           archiveDataPath: String,
           userHistoryPath: String,
           pageHistoryPath: String,
           outputPath: String,
           sqlPartitions: Int
  ): Unit = {

    log.info(s"Denormalized MW Events jobs starting")

    //***********************************
    // Prepare (live-archived) revisions, users and pages history RDDs
    //***********************************

    sqlContext.sql("SET spark.sql.shuffle.partitions=" + sqlPartitions)

    val revisionDf = sqlContext.read.avro(revisionDataPath)
    revisionDf.registerTempTable("revision")

    val archiveDf = sqlContext.read.avro(archiveDataPath)
    archiveDf.registerTempTable("archive")

    val wikiClause = if (wikiConstraint.isEmpty) ""
                     else "AND wiki_db IN (" + wikiConstraint.map(w => s"'$w'").mkString(", ") + ")\n"

    val userStatesDf = sqlContext.read.parquet(userHistoryPath).where(s"TRUE $wikiClause")
    val userStatesToFilter = userStatesDf.rdd.map(UserState.fromRow)
    val userStates = filterStates[UserState](userStatesToFilter, DenormalizedKeysHelper.userStateKeyNoYear)

    val pageStatesDf = sqlContext.read.parquet(pageHistoryPath).where(s"TRUE $wikiClause")
    val pageStatesToFilter = pageStatesDf.rdd.map(PageState.fromRow)
    val pageStates = filterStates[PageState](pageStatesToFilter, DenormalizedKeysHelper.pageStateKeyNoYear)

    val liveRevisions = sqlContext.sql(
      s"""
  SELECT
    wiki_db,
    rev_timestamp,
    rev_comment,
    rev_user,
    rev_user_text,
    rev_page,
    rev_id,
    rev_parent_id,
    rev_minor_edit,
    rev_len,
    rev_sha1,
    rev_content_model,
    rev_content_format
  FROM revision
  WHERE TRUE
    $wikiClause
  -- Trick to force using defined number of partitions
  GROUP BY
    wiki_db,
    rev_timestamp,
    rev_comment,
    rev_user,
    rev_user_text,
    rev_page,
    rev_id,
    rev_parent_id,
    rev_minor_edit,
    rev_len,
    rev_sha1,
    rev_content_model,
    rev_content_format
        """)
      .rdd
      .map(MediawikiEvent.fromRevisionRow)


    val archivedRevisions = sqlContext.sql(
      s"""
  SELECT
    archive.wiki_db,
    ar_timestamp,
    ar_comment,
    ar_user,
    ar_user_text,
    ar_page_id,
    ar_title,
    ar_namespace,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_len,
    ar_sha1,
    ar_content_model,
    ar_content_format
  FROM archive
    -- This is needed to prevent archived revisions having
    -- existing live revisions to cause problem
    FULL OUTER JOIN revision
      ON (archive.wiki_db = revision.wiki_db
        AND archive.ar_rev_id = revision.rev_id)
  WHERE TRUE
    AND revision.rev_id IS NULL
    ${wikiClause.replace("wiki_db", "archive.wiki_db")}
  -- Trick to force using defined number of partitions
  GROUP BY
    archive.wiki_db,
    ar_timestamp,
    ar_comment,
    ar_user,
    ar_user_text,
    ar_page_id,
    ar_title,
    ar_namespace,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_len,
    ar_sha1,
    ar_content_model,
    ar_content_format
      """)
      .rdd
      .map(MediawikiEvent.fromArchiveRow)
      // Remove duplicate revision Ids in archive, use the most recent one
      .keyBy(r => DenormalizedKeysHelper.revisionMediawikiEventKeyNoYear(r).partitionKey)
      .groupByKey()
      .flatMap { case (k, revisionsIterator) =>
        if (k.id > 0) // Keep only most recent revision if multiple
          Seq(revisionsIterator.toVector.sortBy(r => r.eventTimestamp).last)
        else // Don't touch fake Ids
          revisionsIterator.toVector
      }

    val userMediawikiEvents = userStates.map(MediawikiEvent.fromUserState)
    val pageMediawikiEvents = pageStates.map(MediawikiEvent.fromPageState)

    log.info(s"Denormalized MW Events data defined")


    //***********************************
    // Prepare for revisions updates and MW Events denormalization
    //***********************************


    // Partitioners for partition-sort-zip
    val statePartitioner = new PartitionKeyPartitioner[StateKey](sqlPartitions)
    val mediawikiEventPartitioner = new PartitionKeyPartitioner[MediawikiEventKey](sqlPartitions)

    // Partitioned-sorted user and page states for future zipping
    val sortedUserStates = userStates
      .flatMap(s => DenormalizedKeysHelper.userStateKeys(s).map(k => (k, s)))
      .repartitionAndSortWithinPartitions(statePartitioner)
    val sortedPageStates = pageStates
      .flatMap(s => DenormalizedKeysHelper.pageStateKeys(s).map(k => (k, s)))
      .repartitionAndSortWithinPartitions(statePartitioner)

    // user and page iterate functions setup
    val userIterate = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
      MediawikiEvent.updateWithOptionalUser) _
    val pageIterate = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
      MediawikiEvent.updateWithOptionalPage) _

    //***********************************
    // Run revision updates
    //***********************************

    val revisions = DenormalizedRevisionsBuilder
      .run(liveRevisions, archivedRevisions, pageStates, mediawikiEventPartitioner)

    log.info(s"Repartitioning and sorting denormalized revisions, zipping with user states")
    val revisionsWithUserData = revisions
      .keyBy(r => DenormalizedKeysHelper.userMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))
    log.info(s"Revisions with User data: ${revisionsWithUserData.count()}")

    log.info(s"Repartitioning and sorting denormalized revisions, zipping with page states")
    val revisionsWithUserAndPageData = revisionsWithUserData
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedPageStates)((it1, it2) => pageIterate(it1, it2))
    log.info(s"Revisions with User and Page data: ${revisionsWithUserAndPageData.count()}")

    //***********************************
    // Run user and page history updates
    //***********************************

    log.info(s"Repartitioning and sorting denormalized users, zipping with user states")
    val userMediawikiEventsWithUserData = userMediawikiEvents
      .keyBy(u => DenormalizedKeysHelper.userMediawikiEventKey(u))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))
    log.info(s"Users with User data: ${userMediawikiEventsWithUserData.count()}")


    log.info(s"Repartitioning and sorting denormalized pages, zipping with user states")
    val pageMediawikiEventsWithUserData = pageMediawikiEvents
      .keyBy(p => DenormalizedKeysHelper.userMediawikiEventKey(p))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))
    log.info(s"Pages with User data: ${pageMediawikiEventsWithUserData.count()}")

    //***********************************
    // Join revisions, user and page and write result
    //***********************************

    log.info(s"Union-ing denormalized revisions, pages and users, and writing results")
    val denormalizedMediawikiEventsRdd = revisionsWithUserAndPageData
      .union(userMediawikiEventsWithUserData)
      .union(pageMediawikiEventsWithUserData)
    log.info(s"Denormalized revisions, pages and users union: ${denormalizedMediawikiEventsRdd.count()}")

    val denormalizedMediawikiEventsDf = sqlContext.createDataFrame(
        denormalizedMediawikiEventsRdd.map(_.toRow),
        MediawikiEvent.schema)

    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    denormalizedMediawikiEventsDf.write.mode(SaveMode.Overwrite).parquet(outputPath)

    log.info(s"Denormalized MW Events jobs done")

  }
}
