/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HiveDelegationTokenProvider
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.spark.sql.SparkSQLConfig

import java.io._
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/**
  * This code is an updated copy of {{@link org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver}}.
  * The package it is stored in is the original spark one, so that it can access protected and
  * private-to-package spark classes.
  *
  * It allows us to use to run SQL queries in cluster mode by removing the CLI aspect of the
  * driver while keeping the query-from-command and query-from-file running modes.
  *
  * Example usage:
  *
  * spark2-submit --master yarn --deploy-mode cluster \
  * --class org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver \
  * /srv/deployment/analytics/refinery/artifacts/refinery-spark.jar \
  * -f hdfs://path/to/an/hql/query/file.hql
  * -d var1=value1
  * -d var2=value2
  */
object SparkSQLNoCLIDriver extends Logging with ConfigHelper {
    private final val SPARK_HADOOP_PROP_PREFIX = "spark.hadoop."

    case class Config(
        override val database: Option[String] = None,
        override val quoted_query_string: Option[String] = None,
        override val query_file: Option[String] = None,
        override val initialisation_sql_files: Option[Seq[String]] = None,
        override val hiveconf: Option[Map[String, String]] = None,
        override val define: Option[Map[String, String]] = None
    ) extends SparkSQLConfig

    object Config {
        val propertiesDoc: ListMap[String, String] = SparkSQLConfig.propertiesDoc

        val usage: String =
            """
              | Run SparkSQL with input from command-line or file.
              |
              | Example:
              |  spark-submit --class org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                 /my/config/file.properties \
              |   # Override and/or set other configs on the CLI
              |   --define          key:value
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
                    throw new IllegalArgumentException(e)
                }
            }
            log.info("Loaded config:\n" + prettyPrint(config))
            config
        }
    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = Config(args)
        config.validate()

        apply(config.getSparkSQLArgs, new SparkConf(loadDefaults = true))
    }

    def apply(args: Array[String], sparkConf: SparkConf): DataFrame = {
        val oproc = new OptionsProcessor()

        if (!oproc.process_stage1(args)) {
            logError("Problem processing variable-substitution parameters.")
        }

        val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
        val extraConfigs = HiveUtils.formatTimeVarsForHiveClient(hadoopConf)

        val cliConf = new HiveConf(classOf[SessionState])
        (hadoopConf.iterator().asScala.map(kv => kv.getKey -> kv.getValue)
            ++ sparkConf.getAll.toMap ++ extraConfigs).foreach {
            case (k, v) =>
                cliConf.set(k, v)
        }

        val sessionState = new CliSessionState(cliConf)

        if (!oproc.process_stage2(sessionState)) {
            logError("Problem processing command parameters.")
            throw new IllegalArgumentException("Problem processing command parameters.")
        }

        // Set all properties specified via command line.
        val conf = sessionState.getConf
        sessionState.cmdProperties.entrySet().asScala.foreach { item =>
            val key = item.getKey.toString
            val value = item.getValue.toString
            // We do not propagate metastore options to the execution copy of hive.
            if (key != "javax.jdo.option.ConnectionURL") {
                conf.set(key, value)
                sessionState.getOverriddenConfigurations.put(key, value)
            }
        }

        val tokenProvider = new HiveDelegationTokenProvider()
        if (tokenProvider.delegationTokensRequired(sparkConf, hadoopConf)) {
            val credentials = new Credentials()
            tokenProvider.obtainDelegationTokens(hadoopConf, sparkConf, credentials)
            UserGroupInformation.getCurrentUser.addCredentials(credentials)
        }

        SessionState.start(sessionState)

        // Respect the configurations set by --hiveconf from the command line
        // (based on Hive's CliDriver).
        val hiveConfFromCmd = sessionState.getOverriddenConfigurations.entrySet().asScala
        val newHiveConf = hiveConfFromCmd.map { kv =>
            // If the same property is configured by spark.hadoop.xxx, we ignore it and
            // obey settings from spark properties
            val k = kv.getKey
            val v = sys.props.getOrElseUpdate(SPARK_HADOOP_PROP_PREFIX + k, kv.getValue)
            (k, v)
        }

        val noCli = new SparkSQLNoCLIDriver(sparkConf)
        noCli.setHiveVariables(oproc.getHiveVariables)

        if (sessionState.database != null) {
            WMFSparkSQLEnv.sqlContext.sessionState.catalog.setCurrentDatabase(
                s"${sessionState.database}")
        }

        // Execute -i init files (always in silent mode)
        noCli.processInitFiles(sessionState)

        newHiveConf.foreach { kv =>
            WMFSparkSQLEnv.sqlContext.setConf(kv._1, kv._2)
        }

        if (sessionState.execString != null) {
            val returnCode = noCli.processLine(sessionState.execString)
            logInfo(s"Processed SQL query from command line with return code $returnCode")
        } else {
            try {
                if (sessionState.fileName != null) {
                    val returnCode = noCli.processFile(sessionState.fileName)
                    logInfo(s"Processed SQL query from file with return code $returnCode")
                }
            } catch {
                case e: FileNotFoundException =>
                    logError(s"Could not open input file for reading. (${e.getMessage})")
            }
        }
        sessionState.close()
        noCli.getLatestResult
    }

    def isRemoteMode(state: CliSessionState): Boolean = {
        //    sessionState.isRemoteMode
        state.isHiveServerQuery
    }

}

class SparkSQLNoCLIDriver(sparkConf: SparkConf) extends CliDriver with Logging {
    private var latestResult: DataFrame = _

    private val sessionState = SessionState.get().asInstanceOf[CliSessionState]

    private val isRemoteMode = {
        SparkSQLNoCLIDriver.isRemoteMode(sessionState)
    }

    private val conf: Configuration =
        if (sessionState != null) sessionState.getConf else new Configuration()

    // Force initializing WMFSparkSQLEnv. This is put here but not object SparkSQLCliDriver
    // because the Hive unit tests do not go through the main() code path.
    if (!isRemoteMode) {
        WMFSparkSQLEnv.init(sparkConf)
    } else {
        // Hive 1.2 + not supported in CLI
        throw new RuntimeException("Remote operations not supported")
    }

    override def setHiveVariables(hiveVariables: java.util.Map[String, String]): Unit = {
        hiveVariables.asScala.foreach(kv => WMFSparkSQLEnv.sqlContext.conf.setConfString(kv._1, kv._2))
    }

    override def processCmd(cmd: String): Int = {
        val cmd_trimmed: String = cmd.trim()
        val cmd_lower = cmd_trimmed.toLowerCase(Locale.ROOT)
        val tokens: Array[String] = cmd_trimmed.split("\\s+")
        val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()

        if (cmd_lower.equals("quit") ||
            cmd_lower.equals("exit")) {
            logError(s"Can't run command '$cmd_trimmed' in No-CLI mode")
            1
        }
        if (tokens(0).toLowerCase(Locale.ROOT).equals("source") ||
            cmd_trimmed.startsWith("!") || isRemoteMode) {
            logError(s"Can't run command '$cmd_trimmed' in No-CLI mode")
            1
        } else {
            var ret = 0
            val hconf = conf.asInstanceOf[HiveConf]
            val proc: CommandProcessor = CommandProcessorFactory.get(tokens, hconf)
            if (proc != null) {
                if (proc.isInstanceOf[Driver] || proc.isInstanceOf[SetProcessor] ||
                    proc.isInstanceOf[AddResourceProcessor] || proc.isInstanceOf[ListResourceProcessor] ||
                    proc.isInstanceOf[ResetProcessor]) {
                    val driver = new WMFSparkSQLDriver

                    driver.init()
                    val start: Long = System.currentTimeMillis()
                    val rc = driver.run(cmd)
                    val end = System.currentTimeMillis()
                    val timeTaken: Double = (end - start) / 1000.0

                    ret = rc.getResponseCode
                    if (ret != 0) {
                        // For analysis exception, only the error is printed out to the console.
                        rc.getException match {
                            case e: AnalysisException =>
                                logError(s"""Error in query: ${e.getMessage}""")
                            case _ => logError(rc.getErrorMessage)
                        }
                        driver.close()
                        return ret
                    }

                    logInfo(s"Took ${timeTaken}s to execute query ${cmd}")
                    if (driver.getDataFrame != null) {
                        latestResult = driver.getDataFrame
                    }

                    val cret = driver.close()
                    if (ret == 0) {
                        ret = cret
                    }

                    // Destroy the driver to release all the locks.
                    driver.destroy()
                } else {
                    if (sessionState.getIsVerbose) {
                        logError(s"Can't run command '${tokens(0)} $cmd_1' in No-CLI mode")
                    }
                    ret = 1
                }
            }
            ret
        }
    }

    def getLatestResult: DataFrame = latestResult
}
