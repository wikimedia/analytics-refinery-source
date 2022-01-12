package org.wikimedia.analytics.refinery.spark.sql

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

/**
  * This trait is to be used with [[org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver]]
  * stored in refinery-job.
  * It facilitates parameters parsing for the SparkSQLNoCLIDriver in a reusable way, allowing
  * to pass only the needed args to the [[org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver.apply()]]
  * method, and therefore get a dataframe back from a query passed as a file or CLI.
  *
  * See an example in [[org.wikimedia.analytics.refinery.job.HiveToCassandra]].
  */
trait SparkSQLConfig {

    val database: Option[String] = None
    val quoted_query_string: Option[String] = None
    val query_file: Option[String] = None
    val initialisation_sql_files: Option[Seq[String]] = None
    val hiveconf: Option[Map[String, String]] = None
    val define: Option[Map[String, String]] = None

    def getConfigErrors: ArrayBuffer[String] = {
        val illegalArgumentMessages: ArrayBuffer[String] = ArrayBuffer()

        if (quoted_query_string.isEmpty && query_file.isEmpty) {
            illegalArgumentMessages +=
                "No query defined. One quoted_query_string or query_file should be set."
        }
        if (quoted_query_string.isDefined && query_file.isDefined) {
            illegalArgumentMessages +=
                "quoted_query_string and query_file should not be used simultaneously."
        }
        illegalArgumentMessages
    }

    def validate(): Unit = {
        val illegalArgumentMessages = getConfigErrors
        if (illegalArgumentMessages.nonEmpty) {
            throw new IllegalArgumentException(
                illegalArgumentMessages.mkString("\n")
            )
        }
    }

    /**
      * Function re-creating from the parsed arguments the SparkSQL arguments for
      * [[org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver]].
      *
      * @return the parameters to be used by the SparkSQLNoCLIDriver
      */
    def getSparkSQLArgs: Array[String] = {
        val hiveArgs: ArrayBuffer[String] = ArrayBuffer()

        def addStringToHiveArgs(str: Option[String], paramName: String) = {
            if (str.isDefined && str.get.nonEmpty) {
                hiveArgs ++= Seq(paramName, str.get)
            }
        }

        def addMapToHiveArgs(map: Option[Map[String, String]], paramName: String) = {
            if (map.isDefined && map.get.nonEmpty) {
                hiveArgs ++= map.get.flatMap(kv => Seq(paramName, s"${kv._1}=${kv._2}"))
            }
        }

        addStringToHiveArgs(database, "--database")
        addStringToHiveArgs(quoted_query_string, "-e")
        addStringToHiveArgs(query_file, "-f")

        addMapToHiveArgs(hiveconf, "--hiveconf")
        addMapToHiveArgs(define, "--define")

        if (initialisation_sql_files.isDefined && initialisation_sql_files.get.nonEmpty) {
            hiveArgs ++= initialisation_sql_files.get.flatMap(initFile => Seq("-i", initFile))
        }

        hiveArgs.toArray
    }
}

object SparkSQLConfig {

    val propertiesDoc: ListMap[String, String] = ListMap(
        "config_file <file1.properties,files2.properties>" ->
            """comma separated list of paths to properties files.  These files parsed for
              |for matching config parameters as defined here.""",
        "database" ->
            "Specify the database to use",
        "quoted_query_string" ->
            "SQL from command line",
        "query_file" ->
            "SQL from file",
        "initialisation_sql_files" ->
            s"Initialization SQL files (comma-separated)",
        "hiveconf" ->
            "Hive configuration, use value for given property in 'k1:v1,k2:v2' format",
        "define" ->
            "Variable substitution to apply to Hive commands in 'k1:v1,k2:v2' format",
        "help" -> "Print this help message"
    )
}