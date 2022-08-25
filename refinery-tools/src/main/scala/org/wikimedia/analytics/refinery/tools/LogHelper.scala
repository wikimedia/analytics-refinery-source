package org.wikimedia.analytics.refinery.tools

import org.apache.log4j._

/**
 * Helper class to get a logger with a property that can be set on the CLI,
 * named after the extending class.  When used with Refine and spark-submit, you can set
 * --driver-java-options='-Drefinery.log.level=DEBUG' in local client mode,
 * --conf 'spark.executor.extraJavaOptions=-Drefinery.log.level=DEBUG' if using remote executors,
 * --conf 'spark.driver.extraJavaOptions=-Drefinery.log.level=DEBUG' if in remote drive mode (e.g. --master yarn).
 *
 * You can overwrite logLevelConfigName to use a different configuration property
 * to setup logging:
 *
 * class MyExampleClass extends LogHelper {
 * override val logLevelConfigName = "my.example.config.name"
 * }
 *
 */
trait LogHelper {

    val logLevelConfigName = "refinery.log.level"

    lazy val log: Logger = {
        val l = LogManager.getLogger(this.getClass.getName.split('.').last.split('$').last)
        l.setLevel(Level.toLevel(System.getProperty(logLevelConfigName, "INFO"), Level.INFO))
        l
    }

    /**
     * Adds a console appender to the Logger.
     * This is useful if you are using LogHelper without a pre-loaded
     * log4j.properties file (e.g. outside of Spark) and want to see logs
     * on the console.
     *
     * @param pattern PatternLayout pattern string
     * @param target  ConsoleAppender target, either System.out or System.err
     */
    def addConsoleLogAppender(
                                 pattern: String = "%d{yyyy-MM-dd'T'HH:mm:ss.SSS} %p %c{1} %m%n",
                                 target: String = ConsoleAppender.SYSTEM_OUT
                             ): Unit = {
        log.addAppender(
            new ConsoleAppender(new PatternLayout(pattern), ConsoleAppender.SYSTEM_OUT)
        );
    }
}
