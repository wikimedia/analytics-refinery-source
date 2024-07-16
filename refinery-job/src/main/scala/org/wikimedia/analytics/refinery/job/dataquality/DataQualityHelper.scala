package org.wikimedia.analytics.refinery.job.dataquality

import com.amazon.deequ.repository.ResultKey
import org.apache.spark.sql.SparkSession

trait DataQualityHelper {
    def onYarn(spark: SparkSession): Boolean = spark.conf.get("spark.master") == "yarn" // TODO: maybe more generic than this trait

    def mkResultKey(metricType: String): ResultKey = {
        ResultKey(System.currentTimeMillis(), Map("metric_type" -> metricType))
    }
}
