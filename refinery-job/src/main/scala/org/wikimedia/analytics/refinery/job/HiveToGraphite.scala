package org.wikimedia.analytics.refinery.job

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver
import org.wikimedia.analytics.refinery.core.{GraphiteClient,GraphiteMessage, LogHelper}
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.spark.sql.SparkSQLConfig

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

/**
 * Sends results of an HQL query to graphite
 *
 * Important note: the graphite-loading is not parallelized.
 * This means the result of the HQL query needs to be small enough to fit on the spark driver.
 *
 * This job runs a hive query from a hql file using SparkSQLNoCLIDriver.
 *
 * The query is expected to return a Dataframe containing 3 columns,
 * metric, count and timestamp respectively.
 *
 * Ensure the output columns of the HQL Query should have the data types below:
 * metric datatype = String
 * count's datatype = Long
 * timestamp's datatype = timestamp
 *
 * The Dataframe is formatted and
 * sent to graphite using the [[org.wikimedia.analytics.refinery.core.GraphiteClient]] class.
 * This will load the metric, count and time message to Graphite.
 */
object HiveToGraphite extends LogHelper with ConfigHelper {
    /**
     * Config Helper to pick up input parameters for the job
     * It extends from [[org.wikimedia.analytics.refinery.spark.sql.SparkSQLConfig]] trait.
     */
    case class Config(
                     // Overriding the SparkSQLConfig fields for the ConfigHelper to pick them up
                     override val database: Option[String] = None,
                     override val quoted_query_string: Option[String] = None,
                     override val query_file: Option[String] = None,
                     override val initialisation_sql_files: Option[Seq[String]] = None,
                     override val hiveconf: Option[Map[String, String]] = None,
                     override val define: Option[Map[String, String]] = None,
                     //Graphite parameters
                     metric_prefix        : String = "",
                     graphite_host        : String = "",
                     graphite_port        : Int = 2003
                     ) extends SparkSQLConfig {
        /**
         * Validates that Config values provided are correct and reasonable.
         * Otherwise, It throws an exception
         */
        override def validate(): Unit = {
            val illegalArgumentMessages : ArrayBuffer[String] = super.getConfigErrors
            if (graphite_host.isEmpty) {
                illegalArgumentMessages +=
                "The graphite host url shouldn't be empty"
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
        val defaults : Config = Config()

        val usage : String =
            """|Send Hive query result to Graphite.
               |
               |Examples:
               |
               |  spark-submit --class org.wikimedia.analytics.refinery.job.HiveToGraphite refinery-job.jar \
               |   --config_file      /path/to/config/file.properties \
               |   --metric_prefix     test.analytics\
               |   --graphite_host     localhost\
               |   --graphite_port     2003
               |
               |""".stripMargin

        // Set propertiesDoc to be SparkSQLConfig defined propertiesDoc in addition that of the job
        val propertiesDoc:ListMap[String, String] = SparkSQLConfig.propertiesDoc ++ ListMap(
            "metric_prefix" ->
                "Graphite metric namespace/prefix",
            "graphite_host" ->
                "Graphite host url.",
            "graphite_port" ->
                s"Graphite port. Defaults to ${defaults.graphite_port}."
        )

        /**
         * Loads Config from args
         */
        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException =>
                    log.fatal (e.getMessage + ". Aborting.")
                    sys.exit(1)
            }
            log.info("Loaded configuration:\n" + prettyPrint(config))
            config
        }
    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        //Load parameters from args using Config object
        val config = Config(args)
        //validate config parameters provided from args using Config object validate method.
        config.validate()
        //Create a Graphite Socket
        val graphite = new GraphiteClient(config.graphite_host, config.graphite_port)

        // Get the dataframe from the HQL query
        val sparkSQLArgs = config.getSparkSQLArgs
        val sparkConf = new SparkConf(loadDefaults = true)
        log.info(s"Calling SparkSQLNoCLIDriver with arguments: ${sparkSQLArgs.toSeq}")
        val dataframe = SparkSQLNoCLIDriver.apply(sparkSQLArgs, sparkConf)

        if (dataframe == null) {
            val errorMsg = "Dataframe to send into Graphite is null"
            log.error(errorMsg)
            throw new IllegalStateException(errorMsg)
        }
        // Send dataframe to Graphite
        try {
            log.info("Sending metric data generated from HQL query to Graphite.")
            graphite.sendMany(
                dataframe.collect().map(r => {
                    val metric_name = Seq(config.metric_prefix, r.getString(0)).mkString(".")
                    GraphiteMessage(metric_name, r.getLong(1), r.getTimestamp(2).getTime/1000)
                })
            )
            log.info(s"${dataframe.count()} rows of metric data has been sent to Graphite.")
        } catch {
            case e: Exception => log.error(e.getMessage + ". Failed to send message to Graphite.")
        }
    }
}
