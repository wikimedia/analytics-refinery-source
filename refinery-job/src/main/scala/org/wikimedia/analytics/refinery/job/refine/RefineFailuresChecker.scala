package org.wikimedia.analytics.refinery.job.refine

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.LogHelper

/**
  * Use RefineTarget.find to find all Refine targets for an input (camus job) in the last N hours.
  * Then filter for any for which the _REFINED_FAILED flag exists.
  */
object RefineFailuresChecker extends LogHelper {

    val usage: String =
        """
        |Input Datasets -> List of targets with refine failed flag.
        |
        |Given an input base path, this will look for Refine targets that have
        |failed to refine during the time window requested.
        |
        |Example:
        |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineFailuresChecker refinery-job.jar \
        |  # read configs out of this file
        |   --config_file                 /etc/refinery/refine_eventlogging_eventbus.properties \
        |   # Override and/or set other configs on the CLI
        |   --input_path                  /wmf/data/raw/event \
        |   --output_path                 /user/otto/external/eventbus5' \
        |   --output_database             event \
        |   --since                       24 \
        |   --input_path_regex            '.*(eqiad|codfw)_(.+)/hourly/(\d+)/(\d+)/(\d+)/(\d+)'
        |"""

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(Refine.help(usage, Refine.Config.propertiesDoc))
            sys.exit(0)
        }

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        // RefineFailuresChecker mostly uses the same parameters as Refine.
        val refineConfig = Refine.Config(args)

        // Exit non-zero if there were any targets in the time range that have failure flags.
        if (apply(spark)(refineConfig)) {
            sys.exit(0)
        }
        else {
            sys.exit(1)
        }
    }

    def apply(
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    )(
        config: Refine.Config
    ): Boolean = {
        val targets       = Refine.getRefineTargets(spark, config)
        val failedTargets = targets.filter(_.failureFlagExists())
        if (failedTargets.nonEmpty) {
            println("The following targets have the _REFINED_FAILED flag set:")
            failedTargets.foreach(println)
            false
        }
        else {
            println("No target has the _REFINED_FAILED flag set for the selected time window.")
            true
        }
    }
}