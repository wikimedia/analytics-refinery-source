package org.wikimedia.analytics.refinery.core

import org.apache.log4j.{Level, LogManager, Logger}


/**
  * Helper class to get a logger with a property that can be set on the CLI,
  * named after the extending class.  When used with Refine and spark-submit, you can set
  * --driver-java-options='-Djrefine.log.level=DEBUG' in local client mode,
  * --conf 'spark.executor.extraJavaOptions=-Drefine.log.level=DEBUG' if using remote executors,
  * --conf 'spark.driver.extraJavaOptions=-Drefine.log.level=DEBUG' if in remote drive mode (e.g. --master yarn).
  */
trait LogHelper {
    lazy val log: Logger = {
        val l = LogManager.getLogger(this.getClass.getName.split('.').last.split('$').last)
        l.setLevel(Level.toLevel(System.getProperty("refine.log.level", "INFO"), Level.INFO))
        l
    }
}
