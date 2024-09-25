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
/** NOTE: this file is copied from Spark and slightly modified.  The original source is on the 3.1.2 branch
 *  as that is the version we are using at the time of this copy:
 *
 *  https://github.com/apache/spark/blob/v3.1.2/sql/core/src/main/scala/
 *            org/apache/spark/sql/execution/datasources/jdbc/JdbcRelationProvider.scala
 *
 *  Usage:
 *
 *  1. include the jar that contains this file with --jars
 *  2. use just like the jdbc datasource: spark.read.format("mediawiki-jdbc")
 *  3. pass in binary timestamp columns to partition and string dates like:
 *
 *    .option("partitionColumn", "rev_timestamp")
 *    .option("lowerBound", "20240101000000")
 *    .option("upperBound", "20240102000000")
 *    .option("numPartitions", 4)
 */
package org.apache.spark.sql.execution.datasources.custom.jdbc

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.sources.BaseRelation

// -- BEGIN MediaWiki MODIFICATION --
class MediaWikiJdbc extends JdbcRelationProvider {

  override def shortName(): String = "mediawiki-jdbc"
  // -- END MediaWiki MODIFICATION --

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    // -- BEGIN MediaWiki MODIFICATION --
    // use the MediaWikiJdbcRelation object, but not its companion class, JDBCRelation
    //   because all necessary customization is in the object
    val schema = MediaWikiJdbcRelation.getSchema(resolver, jdbcOptions)
    val parts = MediaWikiJdbcRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    // -- END MediaWiki MODIFICATION --
    JDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }
}
