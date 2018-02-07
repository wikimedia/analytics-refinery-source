package org.wikimedia.analytics.refinery.job

import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.Hours
import org.joda.time.format.DateTimeFormatter
import org.wikimedia.analytics.refinery.core.HivePartition
import com.github.nscala_time.time.Imports._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


/**
  * Represents a dataset input and output of a 'refine' job (AKA ETL).  This case class should
  * be used to hold state about an input path and an output HivePartition.  Done and failure
  * flags in the output path are used to indicate the status of a refine job.  If a failure
  * flag exists, some previous job failed.  If a done flag exists, some previous job succeeded.
  * The done flag will have the mtime of the input path directory written into it.  This
  * is used by the shouldRefine method to determine if the input source data has changed
  * since the doneFlag was previously written.  If it has, shouldRefine will return true.
  *
  * Note: 'refine' is not a well defined term.  In general it means a an ETL type job,
  * that takes data from one place, augements it, and outputs it elsewhere.  It usually
  * is expected to be a 1 to 1 mapping of input and output paths, i.e. every input path
  * has an output path.  As such, this probably shouldn't be used for aggregation
  * type jobs, where multiple inputs are mapped to one output.
  *
  * @param fs                   Hadoop FileSystem
  * @param inputPath            Full input partition path
  * @param inputIsSequenceFile  If the input is a Hadoop Sequence File
  * @param partition            HivePartition
  * @param doneFlag             Name of file that should be written upon success of
  *                             the refine job.  This can be created by calling
  *                             the writeDoneFlag method.
  * @param failureFlag          Name of file that should be written upon failure of
  *                             the refine job run.  This can be created by calling
  *
  */
case class RefineTarget(
    fs: FileSystem,
    inputPath: Path,
    inputIsSequenceFile: Boolean,
    partition: HivePartition,
    doneFlag: String,
    failureFlag: String
) {
    /**
      * Easy access to the fully qualified Hive table name.
      */
    val tableName: String = partition.tableName

    /**
      * Easy access to the hive partition path, AKA the output destination path
      */
    val outputPath = new Path(partition.path)

    /**
      * Path to doneFlag in hive table partition output path
      */
    val doneFlagPath = new Path(s"$outputPath/$doneFlag")

    /**
      * Path to doneFlag in hive table partition output path
      */
    val failureFlagPath = new Path(s"$outputPath/$failureFlag")

    /**
      * Number of records successfully refined for this RefineTarget.
      * This should be set using the success method.
      */
    var recordCount: Long = -1

    /**
      * The mtime of the inputPath at the time this RefineTarget is instantiated.
      * Caching this allows us to use the earliest mtime possible to store in doneFlag,
      * in case the inputPath changes while this target is being refined.
      */
    private val inputMTimeCached: Option[DateTime] = inputMTime()

    /**
      * True if the inputPath exists
      * @return
      */
    def inputExists(): Boolean = fs.exists(inputPath)

    /**
      * True if the outputPath exists
      * @return
      */
    def outputExists(): Boolean = fs.exists(outputPath)

    /**
      * True if the outputPath/doneFlag exists
      * @return
      */
    def doneFlagExists(): Boolean = fs.exists(doneFlagPath)

    /**
      * True if the outputPath/failureFlag exists
      * @return
      */
    def failureFlagExists(): Boolean = fs.exists(failureFlagPath)


    /**
      * Returns the mtime Long timestamp of inputPath.  inputPath's
      * mtime will change if it or any of its direct files change.
      * It will not change if a content in a subdirectory changes.
      * @return
      */
    def inputMTime(): Option[DateTime] = {
        if (inputExists()) {
            Some(new DateTime(fs.getFileStatus(inputPath).getModificationTime))
        }
        else
            None
    }


    /**
      *
      * @param path reads a Long timestamp out of path and returns a new DateTime
      * @return DateTime
      */
    private def readMTimeFromFile(path: Path): DateTime = {
        val inStream = fs.open(path)
        val mtime = new DateTime(inStream.readUTF())
        inStream.close()
        mtime
    }


    /**
      * Writes this RefineTarget's mtime to path
      * @param path
      */
    private def writeMTimeToFile(path: Path): Unit = {
        val mtime = inputMTimeCached.getOrElse(
            throw new RuntimeException(
                s"Cannot write mtime to flag file, input mod time was not obtained when $this was " +
                    s"instantiated, probably because it did not exist. This should not happen"
            )
        )

        val outStream = fs.create(path)
        outStream.writeUTF(mtime.toString)
        outStream.close()
    }


    /**
      * Write out doneFlag file for this output target partition
      *
      * This saves the modification timestamp of the inputPath as it when this target was
      * instantiated.  This will allow later comparison of the contents of doneFlag with the
      * inputPath modification time.  If they are different, the user might decide to rerun
      * the refine job for this target, perhaps assuming that there is new
      * data in inputPath.  Note that inputPath directory mod time only changes if
      * its direct content changes, it will not change if something in a subdirectory
      * below it changes.
      */
    def writeDoneFlag(): Unit = {
        writeMTimeToFile(doneFlagPath)
    }


    /**
      * Write out failureFlag file for this output target partition
      *
      * This saves the modification timestamp of the inputPath as it when this target was
      * instantiated.  This will allow later comparison of the contents of failureFlag with the
      * inputPath modification time.
      */
    def writeFailureFlag(): Unit = {
        writeMTimeToFile(failureFlagPath)
    }


    /**
      * Reads the Long timestamp as a DateTime out of the doneFlag
      * @return
      */
    def doneFlagMTime(): Option[DateTime] = {
        if (doneFlagExists()) {
            Some(readMTimeFromFile(doneFlagPath))
        }
        else
            None
    }


    /**
      * Reads the Long timestamp as a DateTime out of the failureFlag
      * @return
      */
    def failureFlagMTime(): Option[DateTime] = {
        if (failureFlagExists()) {
            Some(readMTimeFromFile(failureFlagPath))
        }
        else
            None
    }


    /**
      * This target needs refined if:
      *
      * - The output doesn't exist OR
      * - The output doneFlag doesn't exist or it does and the input mtime has changed OR
      * - The output failureFlag doesn't exist, or it does and we want to ignore previous
      *   failures or the input mtime has changed.
      *
      *
      * The input's mtime has changed if it does not equal the timestamp in the output doneFlag
      * or failureFlag file, meaning that something has changed in the inputPath since the last
      * time the flag file was written.
      *
      * @return
      */
    def shouldRefine(shouldIgnoreFailureFlag: Boolean = false): Boolean = {
        // This could be written and returned as a single boolean conditional statement,
        // keeping track of possible states was confusing.  This is clearer.

        // If the outputExists, check for existent status flag files
        if (outputExists) {
            // If doneFlag exists, and the input mtime has changed, then we need to refine.
            if (doneFlagExists()) {
                return inputMTimeCached != doneFlagMTime()
            }
            // Else if the failure flag exists, we need to refine if
            // we are ignoring the failure flag, or if the input mtime has changed.
            else if (failureFlagExists()) {
                return shouldIgnoreFailureFlag || inputMTimeCached != failureFlagMTime()
            }
        }

        // If none of the above conditions return, we will refine.
        true
    }


    /**
      * Returns a Failure with e wrapped in a new more descriptive Exception
      * @param e Original exception that caused this failure
      * @return
      */
    def failure(e: Exception): Try[RefineTarget] = {
        Failure(RefineTargetException(
            this, s"Failed refinement of $this. Original exception: $e", e
        ))
    }


    /**
      * Returns Success(this) of this RefineTarget
      * @return
      */
    def success(recordCount: Long): Try[RefineTarget] = {
        this.recordCount = recordCount
        Success(this)
    }


    override def toString: String = {
        s"$inputPath -> $partition"
    }
}


object RefineTarget {

    /**
      * Finds RefineTargets with existent input partition paths between sinceDateTime and untilDateTime.
      * The table and partitions are extracted from inputPath by combining inputPathDateTimeFormatter
      * and inputPathRegex.
      *
      * inputPathDateTimeFormatter will be used to construct the expected inputPath for each
      * input partition directory between sinceDateTime and untilDateTime.  E.g. a formatter
      * with a format of "'hourly'/yyyy/MM/dd/HH" will look for existent inputPaths
      * for every hour in the provided time period, like
      *     $baseInputPath/subdir1/hourly/2017/07/26/00,
      *     $baseInputPath/subdir1/hourly/2017/07/26/01,
      *     $baseInputPath/subdir2/hourly/2017/07/26/00,
      *     $baseInputPath/subdir2/hourly/2017/07/26/01,
      * etc.
      *
      * inputPathRegex is expected to capture named groups that include "table" and any other
      * partition keys.  inputPathRegex's capture groups must contain one named "table".
      * E.g. new Regex(
      *     "(eqiad|codfw)_(.+)/hourly/\\d{4}/\\d{2}/\\d{2}/\\d{2}",
      *     "datacenter", "table", "year", "month", "day", "hour"
      *
      * and an inputPath of
      *     $baseInputPath/eqiad_mediawiki_revision-create/2017/07/26/01
      *
      * Will construct a RefineTarget with table "mediawiki_revision_create" (hyphens are converted
      * to underscores) and partitions datacenter="eqiad",year=2017,month=07,day=26,hour=01
      *
      *
      * @param fs                           Hadoop FileSystem
      *
      * @param baseInputPath                Path to base input datasets.  Each subdirectory
      *                                     is assumed to be a unique dataset with individual
      *                                     partitions.  Every subdirectory's partition
      *                                     paths here must be compatible with the provided
      *                                     values of inputPathDateTimeFormatter and inputPathRegex.
      *
      * @param inputIsSequenceFile          Should be True if the input data files are Hadoop
      *                                     Sequence Files.
      *
      * @param baseTableLocationPath        Path to directory where Hive table data will be stored.
      *                                     $baseTableLocationPath/$table will be the value of the
      *                                     external Hive table's LOCATION.
      *
      * @param databaseName                 Hive database name
      *
      * @param doneFlag                     Done flag file.  A successful refinement will
      *                                     write this file to the output path with
      *                                     the Long timestamp of the inputPath's current mod time.
      *
      * @param failureFlag                  Failure flag file.  A failed refinement will
      *                                     write this file to the output path with
      *                                     the Long timestamp of the inputPath's current mod time.
      *
      * @param inputPathDateTimeFormatter   Formatter used to construct input partition paths
      *                                     in the given time range.
      *
      * @param inputPathRegex               Regex used to extract table name and partition
      *                                     information.
      *
      * @param sinceDateTime                Start date time to look for input partitions.
      *
      * @param untilDateTime                End date time to look for input partitions.
      *                                     Defaults to DateTime.now
      * @return
      */
    def find(
        fs: FileSystem,
        baseInputPath: Path,
        inputIsSequenceFile: Boolean,
        baseTableLocationPath: Path,
        databaseName: String,
        doneFlag: String,
        failureFlag: String,
        inputPathDateTimeFormatter: DateTimeFormatter,
        inputPathRegex: Regex,
        sinceDateTime: DateTime,
        untilDateTime: DateTime = DateTime.now
    ): Seq[RefineTarget] = {
        val inputDatasetPaths = subdirectoryPaths(fs, baseInputPath)

        // Map all partitions in each inputPaths since pastCutoffDateTime to RefineTargets
        inputDatasetPaths.flatMap { inputDatasetPath =>
            // Get all possible input partition paths for all directories in inputDatasetPath
            // between sinceDateTime and untilDateTime.
            // This will include all possible partition paths in that time range, even if that path
            // does not actually exist.
            val pastPartitionPaths = partitionPathsSince(
                inputDatasetPath.toString,
                inputPathDateTimeFormatter,
                sinceDateTime,
                untilDateTime
            ).filter { case inputPathRegex => true }

            // Convert each possible partition input path into a possible RefineTarget for refinement.
            pastPartitionPaths.map(partitionPath => {
                // Any capturedKeys other than table are expected to be partition key=values.
                val partition = HivePartition(
                    databaseName,
                    baseTableLocationPath.toString,
                    partitionPath.toString,
                    inputPathRegex
                )

                RefineTarget(
                    fs,
                    partitionPath,
                    inputIsSequenceFile,
                    partition,
                    doneFlag,
                    failureFlag
                )
            })
            // We only care about input partition paths that actually exist,
            // so filter out those that don't.
            .filter(_.inputExists())
        }
    }


    /**
      * Retruns a Seq of all directory Paths in a directory.
      * @param fs           Hadoop FileSystem
      * @param inDirectory  directory Path in which to look for subdirectories
      * @return
      */
    def subdirectoryPaths(fs: FileSystem, inDirectory: Path): Seq[Path] = {
        fs.listStatus(inDirectory).filter(_.isDirectory).map(_.getPath)
    }


    /**
      * Given 2 DateTimes, this generates a Seq of DateTimes representing all hours
      * between since d1 (inclusive) and d2 (exclusive).  E.g.
      *     DateTime.now -> 2017-08-10T21:42:32.820Z
      *
      *     hoursInBetween(DateTime.now - 2.hours, DateTime.now) ->
      *         Seq(2017-08-10T19:00:00.000Z, 2017-08-10T20:00:00.000Z)
      *
      * In the above example, the current hour is 21, and this function returns
      * the previous two hours.
      *
      * @param d1   sinceDateTime
      * @param d2   untilDateTime
      * @return
      */
    def hoursInBetween(d1: DateTime, d2: DateTime): Seq[DateTime] = {
        val oldestHour = new DateTime(d1, DateTimeZone.UTC).hourOfDay.roundCeilingCopy
        val youngestHour = new DateTime(d2, DateTimeZone.UTC).hourOfDay.roundFloorCopy

        for (h <- 0 to Hours.hoursBetween(oldestHour, youngestHour).getHours) yield {
            oldestHour + h.hours - 1.hours
        }
    }


    /**
      * Given a DateTimeFormatter and 2 DateTimes, this will generate
      * a Seq of Paths for every distinct result of fmt.print(hour) prefixed
      * with pathPrefix.  If your date formatter generates the same
      * path for multiple hours, only one of those paths will be included
      * in the result.  This way, you can still generate a list more granular partitions, if
      * your data happens to be partitioned at a more granular time bucketing than hourly.
      *
      * @param pathPrefix   Prefix to prepend to every generated partition path
      * @param fmt          Date formatter to use to extract partition paths from hours
      *                     between d1 and d2
      * @param d1           sinceDateTime
      * @param d2           untilDateTime,  Defaults to DateTime.now
      * @return
      */
    def partitionPathsSince(
        pathPrefix: String,
        fmt: DateTimeFormatter,
        d1: DateTime,
        d2: DateTime = DateTime.now
    ): Seq[Path] = {
        hoursInBetween(d1, d2)
            .map(hour => new Path(pathPrefix + "/" + fmt.print(hour)))
            .distinct
    }
}


/**
  * Exception wrapper used to retrieve the RefineTarget instance from a Failure instance.
  * @param target   RefineTarget
  * @param message  exception message
  * @param cause    Original Exception
  */
case class RefineTargetException(
    target: RefineTarget,
    message: String = "",
    cause: Throwable = None.orNull
) extends Exception(message, cause) { }

