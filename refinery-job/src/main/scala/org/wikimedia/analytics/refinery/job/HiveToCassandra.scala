package org.wikimedia.analytics.refinery.job

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.spark.sql.SparkSQLConfig

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

object HiveToCassandra extends LogHelper with ConfigHelper {

    case class Config(
        // Overriding the SparkSQLConfig fields for the ConfigHelper to pick them up
        override val database: Option[String] = None,
        override val quoted_query_string: Option[String] = None,
        override val query_file: Option[String] = None,
        override val initialisation_sql_files: Option[Seq[String]] = None,
        override val hiveconf: Option[Map[String, String]] = None,
        override val define: Option[Map[String, String]] = None,
        // Cassandra loading fields
        cassandra_columns: Seq[String],
        cassandra_keyspace: String,
        cassandra_table: String = "data",
        cassandra_host: String = "aqs1010-a.eqiad.wmnet:9042",
        cassandra_username: String = "aqsloader",
        cassandra_password: String = "cassandra",
        cassandra_task_parallelism: Int  = 6,
        cassandra_batch_parallelism: Int = 6,
        cassandra_batch_row_number: Int = 1024
    ) extends SparkSQLConfig {
        validate()

        /**
          * Validates that configs as provided make sense.
          * Throws IllegalArgumentException if not.
          */
        override def validate(): Unit = {
            // Check SparkSQL parameters validity
            val illegalArgumentMessages: ArrayBuffer[String] = super.getConfigErrors

            if (cassandra_task_parallelism > 6) {
                illegalArgumentMessages +=
                  "Too high task parallelism for the size of the cassandra cluster - Maximum is 6."
            }
            if (cassandra_task_parallelism < 1) {
                illegalArgumentMessages +=
                  "Too low task parallelism - Minimum is 1."
            }
            if (cassandra_batch_parallelism > 12) {
                illegalArgumentMessages +=
                  "Too high batch parallelism for the size of the cassandra cluster - Maximum is 6."
            }
            if (cassandra_batch_parallelism < 1) {
                illegalArgumentMessages +=
                  "Too low batch parallelism - Minimum is 1."
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
            cassandra_columns = Seq("_COL1_", "_COL2_"),
            cassandra_keyspace = "_CASSANDRA_KEYSPACE_",
            // this is needed for the validate() call of this config not to fail
            query_file = Some("fake_file")
        )

        // Use SparkSQLConfig defined properties in addition the job's ones
        val propertiesDoc: ListMap[String, String] = SparkSQLConfig.propertiesDoc ++ ListMap(
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
            "cassandra_task_parallelism" ->
              s"""the number of spark tasks to load cassandra (between 1 and 6 included).
                 |Default: ${default.cassandra_task_parallelism}""".stripMargin,
            "cassandra_batch_parallelism" ->
              s"""the number of parallel batches per task to load cassandra (between 1 and 12 included).
                 |Default: ${default.cassandra_batch_parallelism}""".stripMargin,
            "cassandra_batch_row_number" ->
              s"""the number of rows to batch together when loading cassandra (default 1024).
                 |Default: ${default.cassandra_batch_row_number}""".stripMargin
        )

        val usage: String =
            """
              | HQL query -> Cassandra
              |
              | Load data from Hive to cassandra using a HQL query.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.HiveToCassandra refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                 /my/config/file.properties \
              |   # Override and/or set other configs on the CLI
              |   --cassandra_keyspace          local_group_default_T_example
              |""".stripMargin

        /**
          * Loads Config from args
          */
        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException => {
                    log.error(e.getMessage + ". Aborting.")
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

        val sparkConf = new SparkConf(loadDefaults = true)
            .set("spark.cassandra.connection.host", config.cassandra_host)
            .set("spark.cassandra.auth.username", config.cassandra_username)
            .set("spark.cassandra.auth.password", config.cassandra_password)
            .set("spark.cassandra.output.batch.size.rows", config.cassandra_batch_row_number.toString)
            .set("spark.cassandra.output.concurrent.writes", config.cassandra_batch_parallelism.toString)

        // Get the dataframe from the HQL query
        val sparkSQLArgs = config.getSparkSQLArgs
        log.info(s"Calling SparkSQLNoCLIDriver with arguments: ${sparkSQLArgs.toSeq}")
        val dataframe = SparkSQLNoCLIDriver.apply(sparkSQLArgs, sparkConf)

        if (dataframe == null) {
            val errorMsg = "Dataframe to load into cassandra is null"
            log.error(errorMsg)
            throw new IllegalStateException(errorMsg)
        }

        // Load data to cassandra
        log.info("Loading cassandra with the HQL query generated dataframe")
        dataframe
          .coalesce(config.cassandra_task_parallelism)
          .rdd
          .saveToCassandra(
              config.cassandra_keyspace,
              config.cassandra_table,
              SomeColumns(config.cassandra_columns.map(c => ColumnName(c)) :_*)
          )
    }

}
