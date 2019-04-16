package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * This class provide spark-sql-view registration for project_namespace_map
 *
 * @param spark the spark session to use
 * @param wikiClause the SQL wiki restriction clause. Should be a valid SQL
 *                   boolean clause based on wiki_db field
 * @param readerFormat The spark reader format to use. Should be one of
 *                     com.databricks.spark.avro, parquet, json, csv
 */
class NamespaceViewRegistrar(
  val spark: SparkSession,
  val wikiClause: String,
  val readerFormat: String
) extends Serializable {

  import org.apache.log4j.Logger

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  /**
   * Register the namespaces view in spark session
   */
  def run(namespacesCsvPath: String): Unit = {

    log.info(s"Registering Namespaces view")

    val namespacesCsvSchema = StructType(
      Seq(StructField("domain", StringType, nullable = false),
        StructField("wiki_db", StringType, nullable = false),
        StructField("namespace", IntegerType, nullable = false),
        StructField("namespace_canonical_name", StringType, nullable = false),
        StructField("namespace_localized_name", StringType, nullable = false),
        StructField("is_content", IntegerType, nullable = false)))

    spark.read
      .schema(namespacesCsvSchema)
      .format(readerFormat)
      .load(namespacesCsvPath)
      .where(s"TRUE $wikiClause")
      .createOrReplaceTempView(SQLHelper.NAMESPACES_VIEW)

    log.info(s"Namespaces view registered")

  }

}
