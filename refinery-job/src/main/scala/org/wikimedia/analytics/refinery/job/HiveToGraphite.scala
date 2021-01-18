package org.wikimedia.analytics.refinery.job

import org.apache.commons.cli.OptionBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver
import org.wikimedia.analytics.refinery.core.{GraphiteClient, GraphiteMessage, LogHelper}
import org.wikimedia.analytics.refinery.hive.HiveCLIOptionsProcessor

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
object HiveToGraphite extends LogHelper {
    /**
     * Config Helper to pick up input parameters for the job
     * It extends from [[HiveCLIOptionsProcessor]].
     */
    class HiveToGraphiteOptionProcessor(args: Array[String]) extends HiveCLIOptionsProcessor {
        //Prepare graphite options to parse

        OptionBuilder.isRequired()
        OptionBuilder.hasArg()
        OptionBuilder.withArgName("host")
        OptionBuilder.withLongOpt("graphite_host")
        OptionBuilder.withDescription("The graphite host to send data to")
        this.addOption(OptionBuilder.create('h'))

        OptionBuilder.hasArg()
        OptionBuilder.withArgName("port")
        OptionBuilder.withLongOpt("graphite_port")
        OptionBuilder.withDescription("The graphite port to use with the host")
        this.addOption(OptionBuilder.create('p'))

        OptionBuilder.hasArg()
        OptionBuilder.withArgName("prefix")
        OptionBuilder.withLongOpt("metric_prefix")
        OptionBuilder.withDescription("The graphite metric prefix to use")
        this.addOption(OptionBuilder.create('m'))

        if (!this.parseArgs(args)) {
            // No need to print the usage, it's done in parseArgs
            sys.exit(1)
        }

        val graphiteHost: String = this.getCommandLine.getOptionValue("graphite_host")
        val graphitePort: Int = {
            val portString = this.getCommandLine.getOptionValue("graphite_port", "2003")
            try {
                Integer.parseInt(portString)
            } catch {
                case e: NumberFormatException =>
                    System.err.println("The graphite port should be an integer value.")
                    this.printUsage()
                    sys.exit(1)
            }
        }
        val graphitePrefix: Option[String] = {
            if (this.getCommandLine.hasOption("metric_prefix")) {
                Some(this.getCommandLine.getOptionValue("metric_prefix", ""))
            } else {
                None
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val optionsProcessor = new  HiveToGraphiteOptionProcessor(args)
        val sparkConf = new SparkConf(loadDefaults = true)

        log.info(s"Calling SparkSQLNoCLIDriver")
        val dataframe = SparkSQLNoCLIDriver.apply(optionsProcessor)
        if (dataframe == null) {
            val errorMsg = "Dataframe to send into Graphite is null"
            log.error(errorMsg)
            throw new IllegalStateException(errorMsg)
        }

        // Send dataframe to Graphite
        try {
            log.info("Sending metric data generated from HQL query to Graphite.")
            val graphite = new GraphiteClient(optionsProcessor.graphiteHost, optionsProcessor.graphitePort)
            val localData = dataframe.collect()
            graphite.sendMany(
                localData.map(r => {
                    val metric_name = (optionsProcessor.graphitePrefix.toSeq :+ r.getString(0)).mkString(".")
                    GraphiteMessage(metric_name, r.getLong(1), r.getTimestamp(2).getTime/1000)
                })
            )
            log.info(s"${localData.length} rows of metric data has been sent to Graphite.")
        } catch {
            case e: Exception => log.error(e.getMessage + ". Failed to send message to Graphite.")
        }
    }
}
