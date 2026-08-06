package org.wikimedia.analytics.refinery.spark.utils

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.tools.LogHelper

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
