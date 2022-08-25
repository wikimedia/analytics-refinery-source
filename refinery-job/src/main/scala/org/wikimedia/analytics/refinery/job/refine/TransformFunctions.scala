package org.wikimedia.analytics.refinery.job.refine

/**
  * This file contains objects with apply methods suitable for passing
  * to Refine to do WMF specific transformations on a DataFrame before
  * inserting into a Hive table.
  *
  * After https://gerrit.wikimedia.org/r/#/c/analytics/refinery/source/+/521563/
  * we are merging JSONSchema with Hive schema before we get to these transforms.
  * This means that if there are additional columns on Hive that are not on
  * the JSON Schema they will already be part of the DataFrame (with null values)
  * when we get to these transform functions.
  *
  * Then, if a transform method is the one that determines the value
  * of this Hive-only column, the transform code needs to drop the column
  * (it holds a null value as it has not been populated with schema values)
  * and re-insert it with the calculated value. See geocode_ip function
  * for an example.
  *
  * See the Refine --transform-functions CLI option documentation.
  */

import java.util.UUID.randomUUID
import scala.util.matching.Regex
import org.apache.spark.sql.functions.{coalesce, col, expr, udf}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.wikimedia.analytics.refinery.core.Webrequest
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive.TransformFunction
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.collection.immutable.ListMap
// These aren't directly used, but are referenced in SQL UDFs.
// Import them to get compile time errors if they aren't available.
import org.wikimedia.analytics.refinery.hive.{GetUAPropertiesUDF, IsSpiderUDF, GetGeoDataUDF, GetHostPropertiesUDF}


/**
  * Helper wrapper to apply a series of transform functions
  * to PartitionedDataFrames containing 'event' data.
  * This just exists to reduce the number of functions
  * we need to provide to Refine's --transform-functions CLI options.
  */
object event_transforms {
    /**
      * Seq of TransformFunctions that will be applied
      * in order to a PartitionedDataFrame.
      */
    val eventTransformFunctions = Seq[TransformFunction](
        remove_canary_events.apply,
        deduplicate.apply,
        geocode_ip.apply,
        parse_user_agent.apply,
        add_is_wmf_domain.apply,
        add_normalized_host.apply
    )

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        eventTransformFunctions.foldLeft(partDf)((currPartDf, fn) => fn(currPartDf))
    }
}


/**
  * Drop duplicate data based on meta.id and/or uuid.
  * - meta.id: Newer (Modern Event Platform) id field.
  * - uuid: Legacy EventLogging Capsule id field.
  *
  * The first non null of these columns will be used as the key for dropping duplicate records.
  * If none of these columns exist, this is a no-op.
  *
  * If the values of all of these columns in a record is null, a temporary uuid
  * will be generated for that record and that will be used for deduplication instead.
  * In this way records with null id values will never be removed as part of deduplication.
  */
object deduplicate extends LogHelper {
    val possibleSourceColumnNames = Seq("meta.id", "uuid")

    /**
      * Generates a new random UUID Column value prefixed with 'fake_'
      */
    val fakeUuid: UserDefinedFunction = udf(
        () => { "fake_" + randomUUID().toString }
    )

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val sourceColumnNames: Seq[String] = partDf.df.findColumnNames(possibleSourceColumnNames)

        // No-op
        if (sourceColumnNames.isEmpty) {
            log.debug(
                s"${partDf.partition} does not contain any id columns named " +
                s"${possibleSourceColumnNames.mkString(" or ")}, cannot deduplicate. Skipping."
            )
            partDf
        } else {
            log.info(s"Dropping duplicates based on ${sourceColumnNames.mkString(",")} columns in ${partDf.partition}")

            // Add fake uuid column into the list of idColumns that will be used, and
            // then coalesce to use the first non null value found.
            // This guarantees that if a record has NULL for all of the possible source id columns,
            // NULL itself will not be considered a unique id.  In that case, the fake uuid will
            // be used as the deduplication key, which will be unique in each row.
            val idColumn = coalesce(sourceColumnNames.map(col) :+ fakeUuid():_*)

            val tempColumnName = s"__temp__uuid__for_deduplicate"
            val tempDf = partDf.df.withColumn(tempColumnName, idColumn)

            partDf.copy(df=tempDf
                // Drop duplicate records based on the tempColumnName we added to the df.
                .dropDuplicates(tempColumnName)
                // And drop the tempColumnName column from the result.
                .drop(tempColumnName)
            )
        }
    }



}

/**
  * Backwards compatible function name for EventLogging refine jobs.
  * This will be removed once event Refine job configurations are unified.
  */
object deduplicate_eventlogging extends LogHelper {
    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        deduplicate(partDf)
    }
}

/**
  * Backwards compatible function name for eventbus refine jobs.
  * This will be removed once event Refine job configurations are unified.
  */
object deduplicate_eventbus extends LogHelper {
    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        deduplicate(partDf)
    }
}

/**
  * Geocodes an ip address column into a new  `geocoded_data` Map[String, String] column
  * using MaxmindDatabaseReaderFactory.
  *
  * The ip address value used will be extracted from one of the following columns:
  * - http.client_ip: Newer Modern Event Platform schemas uses this
  * - ip: legacy eventlogging capsule has this
  * - client_ip: client_ip: used in webrequest table, here just in case something else uses it too.
  *
  * In the order listed above, the first non null column value in the input DataFrame
  * records will be used for geocoding.
  * If none of these fields are present in the DataFrame schema, this is a no-op.
  */
object geocode_ip extends LogHelper {
    val possibleSourceColumnNames = Seq("http.client_ip", "ip", "client_ip")
    val geocodedDataColumnName    = "geocoded_data"

    // eventlogging-processor would stick the client IP into a top level 'ip' column.
    // To be backwards compatible, if this column exists and does not have data, we
    // should fill it with the first non null value of one of the possibleSourceColumnNames.
    val ipLegacyColumnName = "ip"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val spark = partDf.df.sparkSession
        val sourceColumnNames = partDf.df.findColumnNames(possibleSourceColumnNames)

        // No-op
        if (sourceColumnNames.isEmpty) {
            log.debug(
                s"${partDf.partition} does not contain any ip columns named " +
                s"${possibleSourceColumnNames.mkString(" or ")}, cannot geocode. Skipping."
            )
            partDf
        } else {
            // If there is only one possible source column, just
            // use it when geocoding.  Else, we need to COALESCE and use the
            // first non null value chosen from possible source columns in each record.
            val sourceColumnSql = sourceColumnNames match {
                case Seq(singleColumnName) => singleColumnName
                case _ => s"COALESCE(${sourceColumnNames.mkString(",")})"
            }

            log.info(
                s"Geocoding $sourceColumnSql into `$geocodedDataColumnName` in ${partDf.partition}"
            )
            spark.sql(
                "CREATE OR REPLACE TEMPORARY FUNCTION get_geo_data AS " +
                "'org.wikimedia.analytics.refinery.hive.GetGeoDataUDF'"
            )

            // Don't try to geocode records where sourceColumnSql is NULL.
            val geocodedDataSql = s"(CASE WHEN $sourceColumnSql IS NULL THEN NULL ELSE get_geo_data($sourceColumnSql) END) AS $geocodedDataColumnName"

            // Select all columns (except for any pre-existing geocodedDataColumnName) with
            // the result of as geocodedDataSql as geocodedDataColumnName.
            val workingDf = partDf.df
            val columnExpressions = workingDf.columns.filter(
                _.toLowerCase != geocodedDataColumnName.toLowerCase
            ).map(c => s"`$c`") :+ geocodedDataSql
            val parsedDf = workingDf.selectExpr(columnExpressions:_*)


            // If the original DataFrame has a legacy ip, copy the source column value to it
            // for backwards compatibility.
            if (parsedDf.hasColumn(ipLegacyColumnName)) {
                add_legacy_eventlogging_ip(partDf.copy(df = parsedDf), sourceColumnSql)
            }
            else {
                partDf.copy(df = parsedDf)
            }
        }
    }

    /**
      * EventLogging legacy data had an 'ip' column that was set by server side
      * eventlogging-processor.  This function sets it from sourceColumnSql, unless
      * the legacy ip column already has a non null value.
      *
      * @param partDf
      * @param sourceColumnSql
      * @return
      */
    private def add_legacy_eventlogging_ip(
        partDf: PartitionedDataFrame,
        sourceColumnSql: String
    ): PartitionedDataFrame = {
        val spark = partDf.df.sparkSession

        // If ipLegacyColumnName exists and is non NULL, keep it, otherwise set it
        // to the value of sourceColumnSql.
        // This handles the case where we have an (externally eventlogging-processor) parsed ip
        // field, so we shouldn't touch it.
        val ipSql =
            s"""
               |COALESCE(
               |    $ipLegacyColumnName,
               |    $sourceColumnSql
               |) AS $ipLegacyColumnName
               |""".stripMargin

        log.info(
            s"Setting `$ipLegacyColumnName` column in ${partDf.partition} " +
                s"using SQL:\n$ipSql"
        )

        val workingDf = partDf.df

        // Select all columns except for any pre-existing userAgentStructLegacyColumnName with
        // the result of as userAgentStructSql as userAgentStructLegacyColumnName.
        val columnExpressions = workingDf.columns.filter(
            _.toLowerCase != ipLegacyColumnName.toLowerCase
        ).map(c => s"`$c`") :+ ipSql
        partDf.copy(df = workingDf.selectExpr(columnExpressions:_*))
    }
}

/**
  * Parses user agent into the user_agent_map field using
  * org.wikimedia.analytics.refinery.hive.GetUAPropertiesUDF.
  * If the incoming DataFrame schema has a legacy eventlogging `useragent` struct column,
  * it will also be generated using the values in the parsed user_agent_map as well
  * as some extra logic to add the is_mediawiki and is_bot struct fields.
  *
  * The user agent string will be extracted from one of the following columns:
  * - http.request_headers['user-agent']
  *
  * In the order listed above, the first non null column value in the input DataFrame
  * records will be used for user agent parsing.
  * If none of these fields are present in the DataFrame schema, this is a no-op.
  */


object parse_user_agent extends LogHelper {
    // Only one possible source column currently, but we could add more.
    val possibleSourceColumnNames = Seq("http.request_headers.`user-agent`")
    val userAgentMapColumnName    = "user_agent_map"
    // This camelCase userAgent case sensitive name is really a pain.
    // df.hasColumn is case-insenstive, but accessing StructType schema fields
    // by name is not.
    val userAgentStructLegacyColumnName = "useragent"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val spark = partDf.df.sparkSession
        val sourceColumnNames = partDf.df.findColumnNames(possibleSourceColumnNames)

        // No-op
        if (sourceColumnNames.isEmpty) {
            log.debug(
                s"${partDf.partition} does not contain any columns named " +
                s"${possibleSourceColumnNames.mkString(" or ")}. " +
                 "Cannot parse user agent. Skipping."
            )
            partDf
        } else {
            // If there is only one possible source column, just
            // use it when parsing.  Else, we need to COALESCE and use the
            // first non null value chosen from possible source columns in each record.
            val sourceColumnSql = sourceColumnNames match {
                case Seq(singleColumnName) => singleColumnName
                case _ => s"COALESCE(${sourceColumnNames.mkString(",")})"
            }
            log.info(
                s"Parsing $sourceColumnSql into `$userAgentMapColumnName` in ${partDf.partition}"
            )
            spark.sql(
                "CREATE OR REPLACE TEMPORARY FUNCTION get_ua_properties AS " +
                "'org.wikimedia.analytics.refinery.hive.GetUAPropertiesUDF'"
            )
            val userAgentMapSql = s"(CASE WHEN $sourceColumnSql IS NULL THEN NULL ELSE get_ua_properties($sourceColumnSql) END) AS $userAgentMapColumnName"

            // Select all columns except for any pre-existing userAgentMapColumnName with
            // the result of as userAgentMapSql as userAgentMapColumnName.
            val workingDf = partDf.df
            val columnExpressions = workingDf.columns.filter(
                _.toLowerCase != userAgentMapColumnName.toLowerCase
            ).map(c => s"`$c`") :+ userAgentMapSql
            val partDfWithUAMap = partDf.copy(df = workingDf.selectExpr(columnExpressions:_*))

            // If the original DataFrame has a legacy useragent struct field, copy the map field
            // entries to it for backwards compatibility.
            if (should_add_legacy_eventlogging_struct(partDfWithUAMap)) {
                add_legacy_eventlogging_struct(partDfWithUAMap, sourceColumnSql)
            }
            else {
                partDfWithUAMap
            }
        }
    }

    /**
      * If the incoming data has a legacy useragent struct field, OR if
      * the destination Hive table has a legacy useragent struct field, then we should
      * add the struct.  The covers the case where the event schemas do not have
      * userAgentStructLegacyColumnName (usually the case), but the Hive table
      * does.  This is going to be true for all 'legacy' EventLogging datasets
      * that once came through eventlogging-processor.
      * See: https://phabricator.wikimedia.org/T259944#6372923
      *s
      * @param partDf
      * @return
      */
    private def should_add_legacy_eventlogging_struct(partDf: PartitionedDataFrame): Boolean = {
        val spark = partDf.df.sparkSession
        val tableName = partDf.partition.tableName

        // Either the incoming df should already have a struct userAgentStructLegacyColumnName, OR
        // the destination Hive table should.
        // NOTE: dataframe hasColumn is case insensitive, but StructType schema field by name
        // access is not, so when checking that the field is a StructType we need to
        // find the field by name case-insensitively.
        (
            partDf.df.hasColumn(userAgentStructLegacyColumnName) &&
            partDf.df.schema.find(Seq(userAgentStructLegacyColumnName), true).head.isStructType
        ) || (
            spark.catalog.tableExists(tableName) &&
            spark.table(tableName).hasColumn(userAgentStructLegacyColumnName) &&
            spark.table(tableName).schema.find(Seq(userAgentStructLegacyColumnName), true).head.isStructType
        )
    }

    /**
      * eventlogging-processor previously handled user agent parsing and
      * added the 'userAgent' field as a JSON object, which got inferred as a struct
      * in the event schema by Refine jobs.  There may be existent uses of
      * the useragent struct field in Hive, so we need to keep ensuring
      * that a useragent struct exists.  This function generates it from
      * the user_agent_map entries and also adds is_mediawiki and is_bot fields.
      *
      * If a record already has a non NULL useragent struct, it is left intact,
      * otherwise it will be created from the values of user_agent_map.
      *
      * @param partDf input PartitionedDataFrame
      * @param sourceColumnSql SQL string (or just column name) in partDf.df to get user agent string.
      *                     This is passed to the IsSpiderUDF.
      */
    private def add_legacy_eventlogging_struct(
        partDf: PartitionedDataFrame,
        sourceColumnSql: String
    ): PartitionedDataFrame = {
        val spark = partDf.df.sparkSession

        // IsSpiderUDF is used to calculate is_bot value.
        spark.sql(
            "CREATE OR REPLACE TEMPORARY FUNCTION is_spider AS " +
            "'org.wikimedia.analytics.refinery.hive.IsSpiderUDF'"
        )

        // useragent needs to be a struct with these values from UAParser returned user_agent_map,
        // as well as boolean values for is_mediawiki and is_bot.
        // See legacy eventlogging parser code at
        // https://github.com/wikimedia/eventlogging/blob/master/eventlogging/utils.py#L436-L454
        // This is a map from struct column name to SQL that gets the value for that column.
        val userAgentLegacyNamedStructFieldSql = ListMap(
            "browser_family" -> s"$userAgentMapColumnName.`browser_family`",
            "browser_major" -> s"$userAgentMapColumnName.`browser_major`",
            "browser_minor" -> s"$userAgentMapColumnName.`browser_minor`",
            "device_family" -> s"$userAgentMapColumnName.`device_family`",
            // is_bot is true if device_family is Spider, or if device_family is Other and
            // the user agent string matches the spider regex defined in refinery Webrequest.java.
            "is_bot" -> s"""boolean(
                           |            $userAgentMapColumnName.`device_family` = 'Spider' OR (
                           |                $userAgentMapColumnName.`device_family` = 'Other' AND
                           |                is_spider($sourceColumnSql)
                           |            )
                           |        )""".stripMargin,
            // is_mediawiki is true if the user agent string contains 'mediawiki'
            "is_mediawiki" -> s"boolean(lower($sourceColumnSql) LIKE '%mediawiki%')",
            "os_family" -> s"$userAgentMapColumnName.`os_family`",
            "os_major" -> s"$userAgentMapColumnName.`os_major`",
            "os_minor" -> s"$userAgentMapColumnName.`os_minor`",
            "wmf_app_version" -> s"$userAgentMapColumnName.`wmf_app_version`"
        )
        // Convert userAgentLegacyNamedStructFieldSql to a named_struct SQL string.
        val namedStructSql = "named_struct(\n        " + userAgentLegacyNamedStructFieldSql.map({
            case (fieldName, sql) =>
                s"'$fieldName', $sql"
            }).mkString(",\n        ") + "\n    )"

        // Build a SQL statement using named_struct that will generate the
        // userAgentStructLegacyColumnName struct.
        val userAgentMapCaseNonNullSql =
            s"""CASE WHEN $userAgentMapColumnName IS NULL THEN NULL ELSE $namedStructSql END"""

        val userAgentStructSql = if (partDf.df.hasColumn(userAgentStructLegacyColumnName)) {
            // If userAgentStructLegacyColumnName exists and is non NULL, keep it by COALESCEing it.
            // If it doesn't exist on the DF, then we can't refer to it in the SQL, and we don't
            // need to COALESCE anyway.
            // This handles the case where the value of sourceColumnSql might be null, but
            // we have an (externally eventlogging-processor) parsed userAgent EventLogging field,
            // so we don't need to and can't reparse it, since we don't have the original user agent.
            s"""
               |COALESCE(
               |    $userAgentStructLegacyColumnName,
               |    $userAgentMapCaseNonNullSql
               |) AS $userAgentStructLegacyColumnName
               |""".stripMargin
        } else {
            // Else if user_agent_map is NULL, then set useragent to NULL too.
            // Else create useragent struct from user_agent_map
            s"$userAgentMapCaseNonNullSql as $userAgentStructLegacyColumnName"
        }

        log.info(
            s"Adding legacy `$userAgentStructLegacyColumnName` struct column in ${partDf.partition} " +
            s"using SQL:\n$userAgentStructSql"
        )

        val workingDf = partDf.df
        // Select all columns except for any pre-existing userAgentStructLegacyColumnName with
        // the result of as userAgentStructSql as userAgentStructLegacyColumnName.
        val columnExpressions = workingDf.columns.filter(
            _.toLowerCase != userAgentStructLegacyColumnName.toLowerCase
        ).map(c => s"`$c`") :+ userAgentStructSql
        partDf.copy(df = workingDf.selectExpr(columnExpressions:_*))
    }
}


/**
  * Adds an is_wmf_domain column based on the return value of Webrequest.isWMFDomain.
  */
object add_is_wmf_domain extends LogHelper {
    val possibleSourceColumnNames = Seq("meta.domain", "webHost")
    val isWMFDomainColumnName = "is_wmf_domain"

    val isWMFDomain: UserDefinedFunction = udf(
        (hostname: String) => {
            if (hostname == null) {
                false;
            } else {
                Webrequest.isWMFHostname(hostname)
            }
        }
    )

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val sourceColumnNames = partDf.df.findColumnNames(possibleSourceColumnNames)
        val sourceColumnSql = sourceColumnNames match {
            case Seq(singleColumnName) => singleColumnName
            case _ => s"COALESCE(${sourceColumnNames.mkString(",")})"
        }

        partDf.copy(df = partDf.df.withColumn(
            isWMFDomainColumnName, isWMFDomain(expr(sourceColumnSql))
        ))
    }
}


/**
  * Filters out records from a domain that is not a wiki
  * except if those records match domains on a whitelist.
  *
  * Accepted values include:
  *   wikipedia.org, en.wiktionary.org, ro.m.wikibooks,
  *   zh-an.wikivoyage.org, mediawiki.org, www.wikidata.org,
  *   translate.google, etc.
  *
  * Filtered out values include:
  *   en-wiki.org, en.wikipedia.nom.it, en.wikipedi0.org,
  *   www.translatoruser-int.com, etc.
  *
  * Given that domain columns are optional fields we need to accept
  * as valid records for which domain is null.
  *
  * Possible domain columns:
  * - meta.domain: newer (Modern Event Platform) events use this
  * - webHost: legacy EventLogging Capsule data.
  *
  * In the order listed above, the first non null column value in the input DataFrame
  * records will be used for filtering.
  * If none of these fields exist in the input DataFrame schema, this is a no-op.
  */
object filter_allowed_domains extends LogHelper {
    val possibleSourceColumnNames = Seq("meta.domain", "webHost")

    // translate.google ends up sending events we want, so add an exception to keep it.
    var includeList: Regex = List("translate.google").mkString("|").r;

    val isAllowedDomain: UserDefinedFunction = udf(
        (domain: String) => {
            if (domain == null || domain.isEmpty) true
            else if (includeList.findFirstMatchIn(domain.toLowerCase()).isDefined) true
            else if (Webrequest.isWMFHostname(domain)) true
            else false
        }
    )

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        // We don't need to check case insensitively here because
        // column access on a DataFrame is case insensitive already.
        val sourceColumnNames = partDf.df.findColumnNames(possibleSourceColumnNames)

        // No-op
        if (sourceColumnNames.isEmpty) {
            log.debug(
                s"${partDf.partition} does not have any column " +
                s"${possibleSourceColumnNames.mkString(" or ")}, not filtering for allowed domains."
            )
            partDf
        } else {
            // If there is only one possible source column, just
            // use it when filtering.  Else, we need to COALESCE and use the
            // first non null value chosen from possible source columns in each record.
            val sourceColumnSql = sourceColumnNames match {
                case Seq(singleColumnName) => singleColumnName
                case _ => s"COALESCE(${sourceColumnNames.mkString(",")})"
            }
            log.info(
                s"Filtering for allowed domains in $sourceColumnSql in ${partDf.partition}."
            )
            partDf.copy(df = partDf.df.filter(isAllowedDomain(expr(sourceColumnSql))))
        }
    }
}

/**
  * Backwards compatible name of this function for legacy EventLogging jobs.
  * This will be removed once event Refine job configurations are unified.
  */
object eventlogging_filter_is_allowed_hostname extends LogHelper {
    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        filter_allowed_domains.apply(partDf)
    }
}

/**
  * Removes all records where meta.domain == 'canary'.
  * Canary events are fake monitoring events injected
  * into the real event streams by
  * org.wikimedia.analytics.refinery.job.ProduceCanaryEvents.
  */
object remove_canary_events extends LogHelper {
    // We only create canary events with meta.domain == 'canary'
    val sourceColumnName: String = "meta.domain"
    val canaryDomain: String = "canary"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        // No-op
        if (!partDf.df.hasColumn(sourceColumnName)) {
            log.debug(
                s"${partDf.partition} does not have column " +
                s"$sourceColumnName, not removing canary events."
            )
            partDf
        } else {
            log.info(
                s"Filtering for events where $sourceColumnName != '$canaryDomain' in ${partDf.partition}."
            )
            partDf.copy(
                df=partDf.df.where(
                    // I am not sure why we need this IS NULL check here.
                    // In my manual REPL tests, this is not needed (as expected),
                    // but the unit test fails (and removes a NULL meta.domain) if I don't add this.
                    s"$sourceColumnName IS NULL OR $sourceColumnName != '$canaryDomain'"
                )
            )
        }
    }
}

/**
 * Use GetHostPropertiesUDF to get normalized host data from meta.domain and/or webHost
 * and add it as column normalized_host.
 */
object add_normalized_host extends LogHelper {
    val possibleSourceColumnNames = Seq("meta.domain", "webHost")
    val normalizedHostColumnName = "normalized_host"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val spark = partDf.df.sparkSession

        // Use GetHostPropertiesUDF to get normalized host data from meta.domain and/or webHost
        spark.sql(
            "CREATE OR REPLACE TEMPORARY FUNCTION get_host_properties AS " +
              "'org.wikimedia.analytics.refinery.hive.GetHostPropertiesUDF'"
        )

        val sourceColumnNames = partDf.df.findColumnNames(possibleSourceColumnNames)

        val sourceColumnSql = sourceColumnNames match {
            case Seq(singleColumnName) => singleColumnName
            case _ => s"COALESCE(${sourceColumnNames.mkString(",")})"
        }

        partDf.copy(df = partDf.df.withColumn(
            normalizedHostColumnName, expr(s"get_host_properties(${expr(sourceColumnSql)})")
        ))
    }
}
