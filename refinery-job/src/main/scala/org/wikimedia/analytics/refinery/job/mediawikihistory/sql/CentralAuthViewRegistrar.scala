package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class provides spark-sql-view registration for the centralauth_localuser view
 * built on top of wmf_raw.centralauth_localuser table.
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use for the registered view
 * @param readerFormat The spark reader format to use. Should be one of
 *                     avro, parquet, json, csv
 */
class CentralAuthViewRegistrar(
                                val spark: SparkSession,
                                val statsAccumulator: Option[MapAccumulator[String, Long]],
                                val numPartitions: Int,
                                val readerFormat: String
                              ) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  // View names for not reusable views
  private val centralAuthUnprocessedView = "centralauth_unprocessed"

  /**
   * Register the centralauth_localuser view in spark session
   */
  def run(centralAuthUnprocessedPath : String): Unit = {

    // Register needed unprocessed-views
    spark.read.format(readerFormat).load(centralAuthUnprocessedPath).createOrReplaceTempView(centralAuthUnprocessedView)

    log.info(s"Registering centralauth_localuser view")

    // Register the complex view
    spark.sql(s"""
SELECT
  lu_wiki AS wiki_db,
  lu_local_id AS user_id,
  lu_global_id AS user_central_id
FROM ${centralAuthUnprocessedView}
WHERE
  wiki_db = "centralauth"
    """
    ).repartition(numPartitions).createOrReplaceTempView(SQLHelper.CENTRALAUTH_VIEW)

    log.info(s"centralauth_localuser view registered")

  }

}
