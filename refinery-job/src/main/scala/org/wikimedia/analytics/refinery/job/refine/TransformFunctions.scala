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

import scala.util.Random
import scala.util.matching.Regex

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction

import org.wikimedia.analytics.refinery.core.{LogHelper, Webrequest}
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive.TransformFunction
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
// These aren't directly used, but are referenced in SQL UDFs.
// Import them to get compile time errors if they aren't available.
import org.wikimedia.analytics.refinery.hive.{GetUAPropertiesUDF, IsSpiderUDF, GetGeoDataUDF}


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
        deduplicate.apply,
        filter_allowed_domains.apply,
        geocode_ip.apply,
        parse_user_agent.apply
    )

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        eventTransformFunctions.foldLeft(partDf)((currPartDf, fn) => fn(currPartDf))
    }
}


/**
  * Drop duplicate data based on either meta.id or uuid.
  * - meta.id: Newer (Modern Event Platform) id field.
  * - uuid: Legacy EventLogging Capsule id field.
  *
  * If none of these columns exist, this is a no-op.
  */
object deduplicate extends LogHelper {
    val possibleSourceColumnNames = Seq("meta.id", "uuid")

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val sourceColumns = partDf.df.findColumns(possibleSourceColumnNames)

        // No-op
        if (sourceColumns.isEmpty) {
            log.debug(
                s"${partDf.partition} does not contain any id columns named " +
                s"${possibleSourceColumnNames.mkString(" or ")}, cannot deduplicate. Skipping."
            )
            partDf
        } else {
            // Use the first column name found
            val sourceColumn: Column = sourceColumns.head

            // Temporarily add a top level column to the DataFrame
            // with the same values as the sourceColumn.  This allows
            // it to be used as with functions that don't work with
            // nested (struct or map) columns.
            val tempColumnName = "__temp_column_for_deduplicate_" +
                Random.alphanumeric.take(10).mkString

            log.info(s"Dropping duplicates based on `$sourceColumn` column in ${partDf.partition}")
            partDf.copy(df = partDf.df.withColumn(tempColumnName, sourceColumn)
                .dropDuplicates(Seq(tempColumnName))
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
  * The first column found in the input df will be used.
  * If none of these columns exist, this is a no-oop.
  */
object geocode_ip extends LogHelper {
    val possibleSourceColumnNames = Seq("http.client_ip", "ip", "client_ip")
    val geocodedDataColumnName    = "geocoded_data"

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
            // Use the first column name found
            val sourceColumnName = sourceColumnNames.head

            // If the input DataFrame already has a geocodedDataColumnName column, drop it now.
            // We'll re-add it with newly geocoded data as the same name.
            val workingDf = if (partDf.df.hasColumn(geocodedDataColumnName)) {
                log.debug(
                    s"Input DataFrame already has ${geocodedDataColumnName} column;" +
                    s"dropping it before geocoding"
                )
                partDf.df.drop(geocodedDataColumnName)
            }
            else {
                partDf.df
            }

            log.info(
                s"Geocoding `$sourceColumnName` into `$geocodedDataColumnName` in ${partDf.partition}"
            )
            spark.sql(
                "CREATE OR REPLACE TEMPORARY FUNCTION get_geo_data AS " +
                "'org.wikimedia.analytics.refinery.hive.GetGeoDataUDF'"
            )

            // Return a new df with all the original fields plus geocodedDataColumnName.
            partDf.copy(df = workingDf.selectExpr(
                "*", s"get_geo_data($sourceColumnName) AS $geocodedDataColumnName"
            ))
        }
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
  * The first column found in the input df will be used.
  * If none of these fields are present, this is a no-op.
  */
object parse_user_agent extends LogHelper {
    // Only one possible source column currently, but we could add more.
    val possibleSourceColumnNames = Seq("http.request_headers.`user-agent`")
    val userAgentMapColumnName    = "user_agent_map"
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
            // Use the first column name found
            val sourceColumnName = sourceColumnNames.head

            // If the input DataFrame already has a userAgentMapColumnName column, drop it now.
            // We'll re-add it with newly parsed user agent data as the same name.
            val workingDf = if (partDf.df.hasColumn(userAgentMapColumnName)) {
                log.debug(
                    s"Input DataFrame in ${partDf.partition} already has `$userAgentMapColumnName` " +
                    "column. Dropping it before parsing user agent."
                )
                partDf.df.drop(userAgentMapColumnName)
            }
            else {
                partDf.df
            }

            log.info(
                s"Parsing `$sourceColumnName` into `$userAgentMapColumnName` in ${partDf.partition}"
            )
            spark.sql(
                "CREATE OR REPLACE TEMPORARY FUNCTION get_ua_properties AS " +
                "'org.wikimedia.analytics.refinery.hive.GetUAPropertiesUDF'"
            )
            val parsedDf = workingDf.selectExpr(
                "*", s"get_ua_properties($sourceColumnName) AS $userAgentMapColumnName"
            )

            // If the original DataFrame as a legacy useragent struct field, copy the map field
            // entries to it for backwards compatibility.
            if (
                partDf.df.hasColumn(userAgentStructLegacyColumnName) &&
                partDf.df.schema(userAgentStructLegacyColumnName).dataType.isInstanceOf[StructType]
            ) {
                add_legacy_eventlogging_struct(partDf.copy(df = parsedDf), sourceColumnName)
            }
            else {
                partDf.copy(df = parsedDf)
            }
        }
    }


    /**
      * eventlogging-processor previously handled user agent parsing and
      * added the 'userAgent' field as a JSON object, which got inferred as a struct
      * in the event schema by Refine jobs.  There may be existent uses of
      * the useragent struct field in Hive, so we need to keep ensuring
      * that a useragent struct exists.  This function generates it from
      * the user_agent_map entries and also adds is_mediawiki and is_bot fields.
      */
    private def add_legacy_eventlogging_struct(
        partDf: PartitionedDataFrame,
        userAgentStringColumnName: String
    ): PartitionedDataFrame = {
        val spark = partDf.df.sparkSession

        val workingDf = if (partDf.df.hasColumn(userAgentStructLegacyColumnName)) {
            log.debug(
                s"Input DataFrame in ${partDf.partition} already has " +
                s"`${userAgentStructLegacyColumnName}` column. Dropping it before adding " +
                s"legacy eventlogging struct."
            )
            partDf.df.drop(userAgentStructLegacyColumnName)
        }
        else {
            partDf.df
        }

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
        val userAgentLegacyNamedStructFieldSql = Map(
            "browser_family" -> s"$userAgentMapColumnName.`browser_family`",
            "browser_major" -> s"$userAgentMapColumnName.`browser_major`",
            "browser_minor" -> s"$userAgentMapColumnName.`browser_minor`",
            "device_family" -> s"$userAgentMapColumnName.`device_family`",
            "os_family" -> s"$userAgentMapColumnName.`os_family`",
            "os_major" -> s"$userAgentMapColumnName.`os_major`",
            "os_minor" -> s"$userAgentMapColumnName.`os_minor`",
            "wmf_app_version" -> s"$userAgentMapColumnName.`wmf_app_version`",
            // is_mediawiki is true if the user agent string contains 'mediawiki'
            "is_mediawiki" -> s"boolean(lower($userAgentStringColumnName) LIKE '%mediawiki%')",
            // is_bot is true if device_family is Spider, or if device_family is Other and
            // the user agent string matches the spider regex defined in refinery Webrequest.java.
            "is_bot" -> s"""boolean(
                           |$userAgentMapColumnName.`device_family` = 'Spider' OR (
                           |    $userAgentMapColumnName.`device_family` = 'Other' AND
                           |    is_spider($userAgentStringColumnName)
                           |))""".stripMargin
        )

        // Build a SQL statement using named_struct that will generate the
        // userAgentStructLegacyColumnName struct.
        val userAgentStructSql = s"named_struct(" +
            userAgentLegacyNamedStructFieldSql.map(
                { case (fieldName, sql) => s"'$fieldName', $sql"}
            ).mkString(",\n") + s") AS $userAgentStructLegacyColumnName"

        log.info(
            s"Adding legacy `$userAgentStructLegacyColumnName` struct column in ${partDf.partition} " +
            s"using SQL:\n$userAgentStructSql"
        )

        // Return workingDf with all its original fields plus userAgentStructLegacyColumnName with
        // struct fields generated from userAgentStructSql.
        partDf.copy(df = workingDf.selectExpr("*", userAgentStructSql))
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
  * - webHost (and webhost): legacy EventLogging Capsule data.
  *
  * The first column found in the input df will be used.
  * If none of these columns exist, this is a no-op.
  */
object filter_allowed_domains extends LogHelper {

    //Columns are not case sensitive in hive but they are in spark
    val possibleSourceColumnNames = Seq("meta.domain", "webHost", "webhost")

    // TODO: If this changes frequently data for whitelist should
    //       probably come from hive.
    //
    // TODO: These match things like 'fake.translate.google.test.com' or
    //       'fake.www.wikipedia.org.test.com'. Do we want to do this?
    //       s"^(${List("translate.google", "www.wikipedia.org").mkString("|")})$$".r ?s
    var whitelist: Regex = List("translate.google", "www.wikipedia.org").mkString("|").r;

    val isAllowedDomain: UserDefinedFunction = udf((domain:String) => {
        if (domain == null || domain.isEmpty) true
        else if (whitelist.findFirstMatchIn(domain.toLowerCase()).isDefined) true
        else if (Webrequest.isWikimediaHost(domain)) true
        else false
    })

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        val sourceColumns = partDf.df.findColumns(possibleSourceColumnNames)

        // No-op
        if (sourceColumns.isEmpty) {
            log.debug(
                s"${partDf.partition} does not have any column " +
                s"${possibleSourceColumnNames.mkString(" or ")}, not filtering for allowed domains."
            )
            partDf
        } else {
            val sourceColumn = sourceColumns.head
            log.info(
                s"Filtering for allowed domains in `$sourceColumn` column in ${partDf.partition}."
            )
            partDf.copy(df = partDf.df.filter(isAllowedDomain(sourceColumn)))
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
