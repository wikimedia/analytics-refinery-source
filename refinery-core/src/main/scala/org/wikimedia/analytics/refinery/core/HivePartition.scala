package org.wikimedia.analytics.refinery.core

import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import scala.collection.immutable.ListMap
import scala.util.matching.Regex


/**
  * Represents a full Hive table partition.
  * Note: Keeping the order of partition parameters is important to be able to insert
  * data in the right sequence. Hence the ListMap type of the 'partitions' parameter.
  *
  * @param database   Hive database
  * @param t          Hive table (this will be normalized as .table)
  * @param location   Hive table LOCATION path
  * @param partitions ListMap of partition keys -> partition values.
  */
case class HivePartition(
    database: String,
    private val t: String,
    location: String,
    partitions: ListMap[String, Option[String]] = ListMap()
) {
    val table: String = HivePartition.normalize(t)
    val tableName: String = s"`$database`.`$table`"

    /**
      * Seq of partition keys
      */
    val keys: Seq[String] = partitions.keys.toSeq

    /**
      * True is the partition is dynamic (at least one of the partition value is not defined)
      */
    val isDynamic: Boolean = partitions.exists(p => p._2.isEmpty)

    /**
      * A string suitable for use in Hive QL partition operations,
      * e.g. year=2017,month=7,day=12,hour=0
      * or   year=2017,month=7,day=12,hour in case of dynamic partitioning
      */
    val hiveQL: String = {
        HivePartition.mapToHiveQL(partitions, ",")
    }

    /**
      * DateTime of this HivePartition.
      * Only available if this HivePartition has date time keys for which
      * all have concrete values, i.e. none are 'dynamic partition' values.
      */
    val dt: Option[DateTime] = {
        HivePartition.mapToDateTime(partitions)
    }

    /**
      * A relative (LOCATION-less) Hive partition path string.
      * This is how Hive creates partition directories.
      * Throws IllegalStateException if the partition is dynamic
      * as relative-path is not defined as-is but depends on data
      */
    lazy val relativePath: String = {
        if (isDynamic) throw new IllegalStateException("")
        partitions.map { case (k: String, v: Option[String]) =>
            // No need to match None as it is checked above
            v match {
                // If the value looks like a number, strip leading 0s
                case Some(n) if n.forall(_.isDigit) => (k, n.replaceFirst("^0+(?!$)", ""))
                // Else the value should be a string, no need to quote in path.
                case Some(s) => (k, s)
            }
        }
        .map(p => s"${p._1}=${p._2}").mkString("/")
    }

    /**
      * Absolute path to this Hive table partition.
      */
    lazy val path: String = location + "/" + relativePath

    /**
      * A SQL statement to add this partition to tableName, either through explicit
      * addition if the partition is fully defined, or using MSCK REPAIR if the partition
      * is dynamic (partition values are to discovered through folder since they were
      * depending on data at runtime)
      */
    val addPartitionQL: String = {
        if (isDynamic)
            s"MSCK REPAIR TABLE $tableName"
        else
            s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION ($hiveQL) LOCATION '$path'"
    }

    val dropPartitionQL: String = {
        s"ALTER TABLE $tableName DROP IF EXISTS PARTITION ($hiveQL)"
    }

    /**
      * Get a partition value by key
      * @param key partition key
      * @return
      */
    def get(key: String): Option[Option[String]] = {
        partitions.get(key)
    }

    /**
      * True if there are actual partition values defined.
      * @return
      */
    def nonEmpty: Boolean = {
        partitions.nonEmpty
    }

    override def toString: String = {
        s"$tableName $path"
    }
}


/**
  * Companion object with helper constructor via apply.
  * This allows construction of a HivePartition via a String,
  * rather than a ListMap directly.
  */
object HivePartition {

    /**
      * Possible Hive date time partition keys.
      */
    val possibleDateTimePartitionKeys: Seq[String] = Seq("year", "month", "day", "hour")

    /**
      * This regex will be used to replace characters in field names that
      * are likely not compatible in most SQL contexts.
      * This regex uses the Avro rules. https://avro.apache.org/docs/1.8.0/spec.html#names
      */
    val sanitizeFieldPattern: Regex = "(^[^A-Za-z_]|[^A-Za-z0-9_])".r

    /**
      * Normalizes a Hive table or field name using sanitizeFieldPattern.
      * @param name name to normalize
      * @param lowerCase whether to convert the name to lower case.
      * @return
      */
    def normalize(name: String, lowerCase: Boolean = true): String = {
        val sanitizedName = sanitizeFieldPattern.replaceAllIn(name, "_")

        if (lowerCase) {
            sanitizedName.toLowerCase
        } else {
            sanitizedName
        }
    }

    /**
      * This helper constructor will use an extractor regex to get the table and partitions
      * out of a path to extract from, and return a new HivePartition.
      *
      * @param database         Hive database
      * @param baseLocation     Base location to where this table will be stored.  Final table
      *                         location will be baseLocation/table, where table is the extracted
      *                         and normalized table name.
      * @param pathToExtract    path string from which extractorRegex will get table and partitions
      * @param extractorRegex   regex that captures table and partitions from pathToExtract
      * @return
      */
    def apply(
        database: String,
        baseLocation: String,
        pathToExtract: String,
        extractorRegex: Regex
    ): HivePartition = {
        // Get a ListMap of all named groups captured from inputPathRegex
        val capturedKeys = captureListMap(pathToExtract, extractorRegex)
        // "table" MUST be an extracted key.
        val table = normalize(capturedKeys("table").get)
        // The hive table location is baseLocation/table
        val location = baseLocation + "/" + table
        // The partitions are any other key=value pairs groups captured by the regex.
        val partitions = capturedKeys - "table"

        new HivePartition(database, table, location, partitions)
    }

    /**
      * Helper constructor expecting that partitionPath is in the default Hive format,
      * e.g. key1=val1/key2=val2, etc.
      * @param database
      * @param table
      * @param baseLocation
      * @param partitionPath
      * @return
      */
    def apply(
        database: String,
        table: String,
        baseLocation: String,
        partitionPath: String
    ): HivePartition = {
        val location = baseLocation + "/" + table
        new HivePartition(database, table, location, pathToMap(partitionPath))
    }

    /**
      * Helper that assumes that fullPartitonPath contains enough information to
      * create a Hive partition.  The partition path part will start at the first
      * directory that contains an '='.  The table name will be that directory's parent dir,
      * and the database name will be the table name directory's parent dir.
      * @param fullPartitionPath
      * @return
      */
    def apply(fullPartitionPath: String): HivePartition = {
        val pathParts = fullPartitionPath.split("/")

        val partitionPartsIndex = pathParts.indexWhere(_.contains("="))

        if (partitionPartsIndex < 0) {
            throw new RuntimeException(
                s"Cannot create new HivePartition from $fullPartitionPath. " +
                "No Hive style partition directories were found."
            )
        } else if (partitionPartsIndex < 3) {
            throw new RuntimeException(
                s"Cannot create new HivePartition from $fullPartitionPath. " +
                "Expected at least two directories above the first Hive style partition " +
                "from which to infer Hive database and table."
            )
        }

        val table = pathParts(partitionPartsIndex - 1)
        val database = pathParts(partitionPartsIndex - 2)
        val location = pathParts.slice(0, partitionPartsIndex - 1).mkString("/")
        val partitionPath = pathParts.slice(partitionPartsIndex, pathParts.length).mkString("/")

        apply(database, table, location, partitionPath)
    }

    /**
      * Converts a partition path in Hive format to a ListMap.
      * E.g.
      *   key1=val1/key2=val2 -> ListMap(key1 -> val1, key2 -> val2)
      * @param partitionPath Partition location file path
      * @return
      */
    def pathToMap(partitionPath: String): ListMap[String, Option[String]] = {
        val partitionParts = trimString(partitionPath, "/")
            .split("/")
            .filter(_.contains("="))

        partitionParts.foldLeft(ListMap[String, Option[String]]())({
            case (partitionMap, partitionPart) =>
                val keyVal = partitionPart.split("=")
                partitionMap + (keyVal(0) -> Some(keyVal(1)))
        })
    }

    /**
      * Converts a DateTime to a partition ListMap
      * @param dt DateTime
      * @param dtKeys
      *     Seq of partition keys to extract from dt. Only year, month, day, and hour supported.
      * @return
      */
    def dateTimeToMap(
        dt: DateTime,
        dtKeys: Seq[String] = possibleDateTimePartitionKeys
    ): ListMap[String, Option[String]] = {
        dtKeys.foldLeft(ListMap[String, Option[String]]()) ({ case (partitionMap, key) =>
            val value = key match {
                case "year"     => dt.year.get
                case "month"    => dt.monthOfYear.get
                case "day"      => dt.dayOfMonth.get
                case "hour"     => dt.hourOfDay.get
            }
            partitionMap + (key -> Some(value.toString))
        })
    }

    /**
      * Checks if partitions has date time keys, that the partition keys in valid hierarchical
      * order, e.g. month without year is invalid.  If there are no dt keys, this
      * returns false.
      */
    def validateDateTimeKeys(partitions: ListMap[String, Option[String]]): Boolean = {
        partitions.keySet match {
            case keys if keys.contains("hour") => keys.contains("day") && keys.contains("month") && keys.contains("year")
            case keys if keys.contains("day") => keys.contains("month") && keys.contains("year")
            case keys if keys.contains("month") => keys.contains("year")
            case keys => keys.contains("year")
        }
    }

    /**
      * Converts a partition ListMap to a DateTime using dtKeys,
      * if the partition ListMap contains any of the dtKeys, else None.
      */
    def mapToDateTime(
        partitions: ListMap[String, Option[String]],
        dtKeys: Seq[String] = possibleDateTimePartitionKeys
    ): Option[DateTime] = {
        // If the partition dtKeys miss any hierarchical keys, (e.g. month with no year is bad),
        // Or if any of the partition dt key values is None
        // (meaning it is a dynamic partition).
        if (
            !validateDateTimeKeys(partitions) ||
            dtKeys.exists(k => partitions.contains(k) && partitions(k).isEmpty)
        ) {
            None
        } else {
            // Else build a DateTime out of the dtKeys in partitions
            Some(dtKeys.foldLeft(new DateTime(0).withZone(DateTimeZone.UTC))((dt, dtKey) => {
                if (partitions.contains(dtKey)) {
                    val dtValue = partitions(dtKey).get.toInt
                    dtKey match {
                        case "year" => dt.withYear(dtValue)
                        case "month" => dt.withMonthOfYear(dtValue)
                        case "day" => dt.withDayOfMonth(dtValue)
                        case "hour" => dt.withHourOfDay(dtValue)
                        case _ => dt
                    }
                } else {
                    dt
                }
            }))
        }
    }

    /**
      * Converts a partition ListMap to a HiveQL String.
      * @param partitions ListMap of partiton key, value pairs.
      * @param separator
      *     partition separator to use. Depending on the usage, you might want
      *     ",", " AND ", or maybe even "/".
      * @param comparison
      *     String to use for partition key val comparison, e.g. key=val, or key >= val.
      * @return
      */
    def mapToHiveQL(
        partitions: ListMap[String, Option[String]],
        separator: String = ",",
        comparison: String = "="
    ): String = {
        partitions.map({ case (k: String, v: Option[String]) =>
            v match {
                // If the value looks like a number, strip leading 0s
                case Some(n) if n.forall(_.isDigit) => (k, Some(n.replaceFirst("^0+(?!$)", "")))
                // Else the value should be a string, then quote it.
                case Some(s) => (k, Some(s""""$s""""))
                case None => (k, v)
            }
        })
        .map(p => if (p._2.isDefined) s"${p._1}${comparison}${p._2.get}" else s"${p._1}")
        .mkString(separator)
    }

    /**
      * Trims leading and trailing occurrences of toTrim from target string.
      * trimString("////a/b/c/////", "/") -> "a/b/c"
      * @param target string to trim
      * @param toTrim characters to remove from string
      */
    private def trimString(target: String, toTrim: String): String = {
        val trimRegex = s"^$toTrim*(.*[^$toTrim])$toTrim*$$".r
        target match {
            case trimRegex(m) => m
            case _ => target
        }
    }

    /**
      * This will extract the matched regex groupNames and their captured values
      * into a ListMap.  ListMap is used so that order is preserved.
      *
      * @param  s                   String to match
      * @param  regex               Regex with named capture groups
      * @throws RuntimeException    if regex does not match s
      * @return
      */
    private def captureListMap(s: String, regex: Regex): ListMap[String, Option[String]] = {
        val m = regex.findFirstMatchIn(s).getOrElse(
            throw new RuntimeException(
                s"Regex $regex did not match $s when attempting to extract capture group keys"
            )
        )

        m.groupNames.foldLeft(ListMap[String, Option[String]]()) {
            case (currMap, (name: String)) => currMap + (name -> Some(m.group(name)))
        }
    }

    /**
      * Add a mapValues function to ListMap that behaves the same as Map mapValues,
      * but returns a ListMap.
      * @param lm ListMap
      * @tparam K Key type
      * @tparam V1 Value type
      */
    private implicit class ListMapOps[K, V1](lm: ListMap[K, V1]) {
        def listMapValues[V2](f: V1 => V2): ListMap[K, V2] = {
            lm.map { case (k, v1) => (k, f(v1))}
        }
    }

    /**
      * Given 2 DateTimes, build a SQL where condition that satisifies partitions
      * that are between the 2 DateTimes.
      * @param since Since this DateTime inclusive.
      * @param until Until this DateTime exclusive.
      * @param dtKeys Partition key names to use for comparison.
      * @return
      */
    def getBetweenCondition(
        since: DateTime,
        until: DateTime,
        dtKeys: Seq[String] = possibleDateTimePartitionKeys
    ): String = {
        getBetweenCondition(
            dateTimeToMap(since, dtKeys).listMapValues(_.get.toInt),
            dateTimeToMap(until, dtKeys).listMapValues(_.get.toInt)
        )
    }

    /**
      * Returns a string with a SparkSQL condition that can be inserted into
      * a WHERE clause to timely slice a table between two given datetimes,
      * and cause partition pruning (condition can not use CONCAT to compare).
      * The since and until datetimes are passed in the form of ListMaps:
      * ListMap("year" -> 2019, "month" -> 1, "day" -> 1, "hour" -> 0)
      * If the ListMap contains keys that are not one of possibleDateTimePartitionKeys,
      * they will not be used in the resulting condition.
      */
    def getBetweenCondition(
        sinceMap: ListMap[String, Int],
        untilMap: ListMap[String, Int]
    ): String = {
        // Check that partition keys of sinceMap and untilMap match.
        val sincePartitionKeys = sinceMap.keysIterator.toList
        val untilPartitionKeys = untilMap.keysIterator.toList
        if (sincePartitionKeys != untilPartitionKeys) throw new IllegalArgumentException(
            s"since partition keys ($sincePartitionKeys) do not " +
            s"match until partition keys ($untilPartitionKeys)."
        )

        // Get values for current partition.
        val key = sinceMap.head._1
        val since = sinceMap.head._2
        val until = untilMap.head._2

        // Check that since is smaller than until.
        if (since > until) throw new IllegalArgumentException(
            s"since ($since) greater than until ($until) for partition key '$key'."
        )

        if (since == until) {
            // Check that since does not equal until for last partition.
            // Nothing would fulfill the condition, because until is exclusive.
            if (sinceMap.size == 1) throw new IllegalArgumentException(
                s"since equal to until ($since) for last partition key '$key'."
            )

            // If since equals until for a given partition key, specify that
            // in the condition and AND it with the recursive call to generate
            // the between condition for further partitions.
            s"$key = $since AND " + getBetweenCondition(sinceMap.tail, untilMap.tail)
        } else {
            // If since is smaller than until, AND two threshold conditions,
            // one to specify greater than since, and another to specify
            // smaller than until.
            getThresholdCondition(sinceMap, ">") +
            " AND " +
            getThresholdCondition(untilMap, "<")
        }
    }

    /**
      * Returns a string with a SparkSQL condition that can be inserted into
      * a WHERE clause to timely slice a table below or above a given datetime,
      * and cause partition pruning (condition can not use CONCAT to compare).
      * The threshold datetime is passed in the form of a ListMap:
      * ListMap("year" -> 2019, "month" -> 1, "day" -> 1, "hour" -> 0)
      * The order parameter determines whether the condition should accept
      * values above (>) or below (<) the threshold.
      */
    def getThresholdCondition(
        thresholdMap: ListMap[String, Int],
        comparison: String
    ): String = {
        val key = thresholdMap.head._1
        val threshold = thresholdMap.head._2

        if (thresholdMap.size == 1) {
            // If there's only one partition key to compare,
            // output a simple comparison expression.
            // Note: > becomes inclusive, while < remains exclusive.
            if (comparison == ">") s"$key >= $threshold" else s"$key < $threshold"
        } else {
            // If there's more than one partition key to compare,
            // output a condition that covers the following 2 cases:
            // 1) The case where the value for the current partition is
            //    exclusively smaller (or greater) than the threshold
            //    (further partitions are irrelevant).
            // 2) The case where the value for the current partition equals
            //    the threshold, provided that further partitions fulfill the
            //    condition created by the corresponding recursive call.
            s"($key $comparison $threshold OR $key = $threshold AND " +
            getThresholdCondition(thresholdMap.tail, comparison) +
            ")"
        }
    }

    /**
      * Returns a string with a SparkSQL condition that can be inserted into
      * a WHERE clause to timely slice a snapshot-based table at the given
      * snapshot interval and cause partition pruning. Only weekly and monthly
      * snapshot frequencies are supported. To deduce the frequency, the gap
      * between since and until is used. For instance:
      *   since="2023-01-01", until="2023-02-01" -> "snapshot = '2023-01'"
      *   since="2023-05-01", until="2023-05-08" -> "snapshot = '2023-05-01'"
      */
    def getSnapshotCondition(
        since: DateTime,
        until: DateTime
    ): String = {
        val formatter = if (since.plusWeeks(1) == until) {
            DateTimeFormat.forPattern("yyyy-MM-dd")
        } else if (since.plusMonths(1) == until) {
            DateTimeFormat.forPattern("yyyy-MM")
        } else {
            throw new IllegalArgumentException(
                "The only supported snapshot frequencies are weekly and monthly."
            )
        }
        s"snapshot = '${formatter.print(since)}'"
    }
}
