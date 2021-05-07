package org.wikimedia.analytics.refinery.job

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.core.config._

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

object HiveToCassandra extends LogHelper with ConfigHelper {

    case class Config(
        hql_query: String,
        cassandra_columns: Seq[String],
        cassandra_keyspace: String,
        cassandra_table: String = "data",
        cassandra_host: String = "aqs1010-a.eqiad.wmnet:9042",
        cassandra_username: String = "aqsloader",
        cassandra_password: String = "cassandra",
        cassandra_load_parallelism: Int  = 6
    ) {
        validate()

        /**
          * Validates that configs as provided make sense.
          * Throws IllegalArgumentException if not.
          */
        private def validate(): Unit = {
            val illegalArgumentMessages: ArrayBuffer[String] = ArrayBuffer()

            if (cassandra_load_parallelism > 6) {
                illegalArgumentMessages +=
                  "Too high parallelism for the size of the cassandra cluster - Maximum is 6."
            }
            if (cassandra_load_parallelism < 1) {
                illegalArgumentMessages +=
                  "Too low parallelism - Minimum is 1."
            }
            if (hql_query.isEmpty) {
                illegalArgumentMessages +=
                  "The HQL query shouldn't be empty."
            }
            if (cassandra_columns.isEmpty) {
                illegalArgumentMessages +=
                  "The CQL columns shouldn't be empty."
            }
            if (cassandra_keyspace.isEmpty) {
                illegalArgumentMessages +=
                  "The cassandra keyspace shouldn't be empty."
            }

            if (illegalArgumentMessages.nonEmpty) {
                throw new IllegalArgumentException(
                    illegalArgumentMessages.mkString("\n")
                )
            }
        }
    }

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values so that validate() will succeed.
        val default: Config = Config(
            hql_query = "_SELECT .. FROM ..._",
            cassandra_columns = Seq("_COL1_", "_COL2_"),
            cassandra_keyspace = "_CASSANDRA_KEYSPACE_"
        )

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
              """comma separated list of paths to properties files.  These files parsed for
                |for matching config parameters as defined here.""",
            "hql_query" ->
              "the Hive query to be run to extract and format data to be loaded in Cassandra.",
            "cassandra_columns" ->
              "the name of the cassandra columns to be loaded, in same order they are defined in the hive query.",
            "cassandra_keyspace" ->
              "the cassandra keyspace to use.",
            "cassandra_table" ->
              s"the cassandra table to use. Default: ${default.cassandra_table}",
            "cassandra_host" ->
              s"""the cassandra host with port to connect to the cluster (default cassandra port is used if not specified).
                 |A comma-separated list of host:port may be used. Default: ${default.cassandra_host}""".stripMargin,
            "cassandra_username" ->
              s"the cassandra username to use to connect to the cluster. Default: ${default.cassandra_username}",
            "cassandra_password" ->
              s"the cassandra password to use to connect to the cluster. Default: ${default.cassandra_password}",
            "cassandra_load_parallelism" ->
              s"""the parallelism to use to load cassandra (between 1 and 6 included).
                 |Default: ${default.cassandra_load_parallelism}""".stripMargin
        )

        val usage: String =
            """
              | HQL query -> Cassandra
              |
              | Load data from Hive to cassandra running a HQL query.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.HiveToCassandra refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                 /my/config/file.properties \
              |   # Override and/or set other configs on the CLI
              |   --cassandra_keyspace          local_group_default_T_example
              |"""

        /**
          * Loads Config from args
          */
        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException => {
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
                }
            }
            log.info("Loaded HiveToCassandra config:\n" + prettyPrint(config))
            config
        }
    }


    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = Config(args)

        val spark: SparkSession = SparkSession.builder()
          .enableHiveSupport()
          .config("spark.cassandra.connection.host", config.cassandra_host)
          .config("spark.cassandra.auth.username", config.cassandra_username)
          .config("spark.cassandra.auth.password", config.cassandra_password)
          .getOrCreate()

        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        spark.sql(config.hql_query)
          .coalesce(config.cassandra_load_parallelism)
          .rdd
          .saveToCassandra(
              config.cassandra_keyspace,
              config.cassandra_table,
              SomeColumns(config.cassandra_columns.map(c => ColumnName(c)) :_*)
          )
    }

}
