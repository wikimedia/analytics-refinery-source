package org.wikimedia.analytics.refinery.spark.utils

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.tools.LogHelper

/**
 * Spark 3.5 twin of the same trait in refinery-spark.
 *
 * refinery-spark is compiled against Spark 3.1, so this module cannot depend on
 * it. The trait is small enough that copying it is cheaper than a shared module
 * built twice; keep the two in step if either changes.
 */
trait SparkLogHelper extends LogHelper {

    /**
     * If not running in yarn, make spark log level quieter.
     * @param spark
     */
    def adjustLoggingLevelToSparkMaster(spark: SparkSession): Unit = {
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }
    }

}
