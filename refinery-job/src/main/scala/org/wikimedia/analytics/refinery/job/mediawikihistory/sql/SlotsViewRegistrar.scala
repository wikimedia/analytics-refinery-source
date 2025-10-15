package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.sql.MediawikiMultiContentRevisionSha1
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}


/**
 * This class provides spark-sql-view registration for slots and related slot_role and content
 *
 * @param spark            the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions    the number of partitions to use for the registered view
 * @param wikiClause       the SQL wiki restriction clause. Should be a valid SQL
 *                         boolean clause based on wiki_db field
 * @param readerFormat     The spark reader format to use. Should be one of
 *                         avro, parquet, json, csv
 */
class SlotsViewRegistrar(
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
    private val contentUnprocessedView = "content_unprocessed"
    private val slotRolesUnprocessedView = "slot_roles_unprocessed"
    private val slotsUnprocessedView = "slots_unprocessed"

    /**
     * Register the slots view in spark session joining the slots unprocessed table
     * to the slot_role one for role resolution using the broadcast join trick. Also joins
     * to content to get content metadata.
     */
    def run(
        contentUnprocessedPath: String,
        slotRolesUnprocessedPath: String,
        slotsUnprocessedPath: String
    ): Unit = {

        log.info(s"Registering Slots view")

        // Register needed unprocessed-views
        spark.read.format(readerFormat).load(contentUnprocessedPath).createOrReplaceTempView(contentUnprocessedView)
        spark.read.format(readerFormat).load(slotRolesUnprocessedPath).createOrReplaceTempView(slotRolesUnprocessedView)
        spark.read.format(readerFormat).load(slotsUnprocessedPath).createOrReplaceTempView(slotsUnprocessedView)

        // Register complex view
        spark.sql(
            s"""
WITH filtered_slots AS (
    SELECT
        wiki_db,
        slot_revision_id,
        slot_role_id,
        slot_content_id
    FROM $slotsUnprocessedView
    WHERE TRUE
        $wikiClause
),

filtered_slot_roles AS (
    SELECT
        wiki_db,
        role_id,
        role_name
    FROM $slotRolesUnprocessedView
    WHERE TRUE
        $wikiClause
),

filtered_content AS (
    SELECT
        wiki_db,
        content_id,
        content_sha1
    FROM $contentUnprocessedView
    WHERE TRUE
        $wikiClause
)

SELECT
  slots.wiki_db,
  slot_revision_id,
  slot_roles.role_name,
  content.content_sha1

FROM filtered_slots slots
  LEFT JOIN filtered_slot_roles slot_roles
    ON slots.wiki_db = slot_roles.wiki_db
      AND slots.slot_role_id = slot_roles.role_id
  LEFT JOIN filtered_content content
    ON slots.wiki_db = content.wiki_db
      AND slots.slot_content_id = content.content_id
    """
        ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.SLOTS_VIEW)

        log.info(s"Slots view registered")

    }

}
