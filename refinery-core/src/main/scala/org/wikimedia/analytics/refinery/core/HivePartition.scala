package org.wikimedia.analytics.refinery.core

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
    partitions: ListMap[String, String] = ListMap()
) {
    val table: String = HivePartition.normalize(t)
    val tableName: String = s"`$database`.`$table`"

    /**
      * Seq of partition keys
      */
    val keys: Seq[String] = partitions.keys.toSeq

    /**
      * A string suitable for use in Hive QL partition operations,
      * e.g. year=2017,month=7,day=12,hour=0
      */
    val hiveQL: String = {
        partitions.map { case (k: String, v: String) =>
            v match {
                // If the value looks like a number, strip leading 0s
                case n if n.forall(_.isDigit) => (k, n.replaceFirst("^0+(?!$)", ""))
                // Else the value should be a string, then quote it.
                case s => (k, s""""$s"""")
            }
        }
        .map(p => s"${p._1}=${p._2}").mkString(",")
    }

    /**
      * A relative (LOCATION-less) Hive partition path string.
      * This is how Hive creates partition directories.
      */
    val relativePath: String = {
        partitions.map { case (k: String, v: String) =>
            v match {
                // If the value looks like a number, strip leading 0s
                case n if n.forall(_.isDigit) => (k, n.replaceFirst("^0+(?!$)", ""))
                // Else the value should be a string, no need to quote in path.
                case s => (k, s)
            }
        }
        .map(p => s"${p._1}=${p._2}").mkString("/")
    }

    /**
      * Absolute path to this Hive table partition.
      */
    val path: String = location + "/" + relativePath

    /**
      * A SQL statement to add this partition to tableName.
      */
    val addPartitionQL: String = s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION ($hiveQL) LOCATION '$path'"

    /**
      * Get a partition value by key
      * @param key partition key
      * @return
      */
    def get(key: String): Option[String] = {
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
        s"$tableName ($hiveQL)"
    }
}


/**
  * Companion object with helper constructor via apply.
  * This allows construction of a HivePartition via a String,
  * rather than a ListMap directly.
  */
object HivePartition {
    /**
      *
      * @param s
      * @return
      */
    def normalize(s: String): String = {
        s.replace("-", "_")
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
        val table = normalize(capturedKeys("table"))
        // The hive table location is baseLocation/table
        val location = baseLocation + "/" + table
        // The partitions are any other key=value pairs groups captured by the regex.
        val partitions = capturedKeys - "table"

        new HivePartition(database, table, location, partitions)
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
    private def captureListMap(s: String, regex: Regex): ListMap[String, String] = {
        val m = regex.findFirstMatchIn(s).getOrElse(
            throw new RuntimeException(
                s"Regex $regex did not match $s when attempting to extract capture group keys"
            )
        )

        m.groupNames.foldLeft(ListMap[String, String]()) {
            case (currMap, (name: String)) => currMap ++ ListMap(name -> m.group(name))
        }
    }

}
