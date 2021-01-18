package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This code is an updated copy of {{@link org.apache.spark.sql.hive.thriftserver.SparkSQLEnv}}.
  *
  * It allows us to initialize the SparkSQL environment with an existing SparkConf
  */
private[hive] object WMFSparkSQLEnv extends Logging {
    logDebug("Initializing SparkSQLEnv")

    var sqlContext: SQLContext = _
    var sparkContext: SparkContext = _

    def init(): Unit = {
        if (sqlContext == null) {
            val sparkConf = new SparkConf(loadDefaults = true)
            
            val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
            sparkContext = sparkSession.sparkContext
            sqlContext = sparkSession.sqlContext

            // SPARK-29604: force initialization of the session state with the Spark class loader,
            // instead of having it happen during the initialization of the Hive client (which may use a
            // different class loader).
            sparkSession.sessionState
        }
    }

    /** Cleans up and shuts down the Spark SQL environments. */
    def stop() {
        logDebug("Shutting down Spark SQL Environment")
        // Stop the SparkContext
        if (WMFSparkSQLEnv.sparkContext != null) {
            sparkContext.stop()
            sparkContext = null
            sqlContext = null
        }
    }
}
