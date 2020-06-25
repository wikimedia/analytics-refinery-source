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
        partitions.map { case (k: String, v: Option[String]) =>
            v match {
                // If the value looks like a number, strip leading 0s
                case Some(n) if n.forall(_.isDigit) => (k, Some(n.replaceFirst("^0+(?!$)", "")))
                // Else the value should be a string, then quote it.
                case Some(s) => (k, Some(s""""$s""""))
                case None => (k, v)
            }
        }
        .map(p => if (p._2.isDefined) s"${p._1}=${p._2.get}" else s"${p._1}")
        .mkString(",")
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
      * @param s The string to normalize
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
        new HivePartition(database, table, location, partitionPathToListMap(partitionPath))
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
      * @param partitionPath
      * @return
      */
    def partitionPathToListMap(partitionPath: String): ListMap[String, Option[String]] = {
        val partitionParts = trimString(partitionPath, "/").split("/")
        partitionParts.foldLeft(ListMap[String, Option[String]]())({
            case (partitionMap, partitionPart) =>
                val keyVal = partitionPart.split("=")
                partitionMap + (keyVal(0) -> Some(keyVal(1)))
        })
    }

    /**
      * Trims leading and trailing occurences of toTrim from target string.
      * trimString("////a/b/c/////", "/") -> "a/b/c"
      * @param target
      * @param toTrim
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

}
