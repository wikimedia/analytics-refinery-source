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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}

/**
  * This code is an updated copy of {{@link org.apache.spark.sql.hive.thriftserver.SparkSQLDriver}}.
  *
  * It allows us to use to get a resulting DataFrame from a query instead of Hive returned values.
  */
private[hive] class WMFSparkSQLDriver(val context: SQLContext = WMFSparkSQLEnv.sqlContext) extends Driver with Logging {

    private var response: DataFrame = _

    override def init(): Unit = {
    }

    override def run(command: String): CommandProcessorResponse = {
        // TODO unify the error code
        try {
            response = context.sql(command)

            new CommandProcessorResponse(0)
        } catch {
            case ae: AnalysisException =>
                logDebug(s"Failed in [$command]", ae)
                new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(ae), null, ae)
            case cause: Throwable =>
                logError(s"Failed in [$command]", cause)
                new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(cause), null, cause)
        }
    }

    def getDataFrame: DataFrame = {
        response
    }

    override def close(): Int = {
        response = null
        0
    }

    override def destroy() {
        super.destroy()
        response = null
    }
}
