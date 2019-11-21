package org.wikimedia.analytics.refinery.job

import java.io.IOException
import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.Trash
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.core.config._

import scala.collection.immutable.ListMap

/**
  * Aids in deleting files older than a given mtime.
  * Useful for cleaning the HDFS tmp directory in HDFS.
  */
object HDFSCleaner extends LogHelper with ConfigHelper {

    /**
      * Abort if a path to clean is ever given a path that exactly
      * matches one of these.  This just a paranoid
      * safety check to make sure we don't accidentally
      * delete things we don't want to.
      * @type {[type]}
      */
    val disallowedPaths = Seq(
        "/", "/wmf", "/wmf/data", "/wmf/camus", "/wmf/discovery", "/user"
    )

    /**
      * Config class for use config files and args.
      */
    case class Config(
        path: String,
        older_than_seconds: Long,
        skip_trash: Boolean = false,
        dry_run: Boolean    = false
    )

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        log.info("Loaded configuration:\n" + prettyPrint(config))
        config
    }

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        val default = Config("", 0L)

        val propertiesDoc: ListMap[String, String] = ListMap(
            "path" -> "Path in which to clean up old files.",
            "older_than_seconds" -> "Clean up files with mtimes older than this.",
            "skip_trash" ->
                s"""If true, files will be deleted directly, else they will be moved
                   |to a .Trash dir.  Default: ${default.skip_trash}""",
            "dry_run" ->
                s"""Don't actually clean up any files, just count them.
                   |Default: ${default.dry_run}"""
        )

        val usage: String =
            """
            |Recursively cleans up files with old mtimes in a directory.
            |
            |This is useful for cleaning out temp directories.
            |
            |Example:
            |  # Move files older than 31 days in /tmp to trash
            |  java -cp refinery-job.jar:$(/usr/bin/hadoop classpath) org.wikimedia.analytics.refinery.job.HDFSCleaner \
            |      --path=/tmp \
            |      --older_than_seconds=2678400
            |"""
    }



    /**
      * Iterates over files and directories depth first calling
      * callback first on leaf files then on containing directories.
      * Uses an Hadoop FileSystem RemoteIterator to avoid getting path's entire
      * directory tree at once.
      *
      * @param fs           FileSystem
      * @param path         Path to start in
      * @param callback     Will be called for every file and directory
      *                     Should return the number of files operated on by the callback
      * @param recursive    True if child directories should be recursed into
      * @return
      */
    def apply(fs: FileSystem,
        path: Path,
        callback: (FileSystem, FileStatus) => Long,
        recursive: Boolean = true
    ): Long = {
        var appliedCount = 0L

        if (isAllowedPath(path)) {
            val iterator = try {
                Some(fs.listLocatedStatus(path))
            } catch {
                case tolerated: FileNotFoundException =>
                    log.warn(s"Trying to ls $path, but it no longer exists, skipping...")
                    None
                case unexpected: Exception => throw unexpected
            }

            if (iterator.isDefined) {
                // These two lines were used to test the exception catching
                // in case of external file deletion while the program runs.
                //   println(s"Content of folder $path is in memory")
                //   scala.io.StdIn.readLine()

                while (iterator.get.hasNext) {
                    val nextFile = iterator.get.next
                    val nextPath = nextFile.getPath

                    // If recursing and we've got a directory, recurse into it and apply callback
                    // to all enclosed files and directories before moving on.
                    if (recursive && nextFile.isDirectory) {
                        appliedCount = appliedCount + apply(fs, nextPath, callback)
                    }
                    // Apply callback to current file or directory
                    val callbackCount = try {
                        callback(fs, nextFile)
                    } catch {
                        case tolerated: FileNotFoundException =>
                            log.warn(s"Trying to apply callback to $nextPath, but it no longer exists, skipping...")
                            0L
                        case unexpected: Exception => throw unexpected
                    }
                    appliedCount = appliedCount + callbackCount
                }
            }
        } else {
            log.warn(s"HDFSCleaner is not allowed to be applied to $path")
        }
        appliedCount
    }

    /**
      * Returns true if path is not one of the disallowedPathsForDeletion
      * @param path
      * @return
      */
    def isAllowedPath(path: Path): Boolean = {
        val pathString = Path.getPathWithoutSchemeAndAuthority(path).toString
        !disallowedPaths.contains(pathString)
    }

    /**
      * True if path does not contain any files or directories.
      * Files are always considered empty paths.
      * @param fs
      * @param path
      * @return
      */
    def isEmpty(fs: FileSystem, fileStatus: FileStatus): Boolean = {
        fileStatus.isFile() || !fs.listFiles(fileStatus.getPath, false).hasNext
    }

    /**
      * Either deletes or trashes path.
      * If path is a directory, it will be recursively deleted.
      *
      * @param fs
      * @param path
      * @param skipTrash
      * @return
      */
    def deleteOrTrash(fs: FileSystem, path: Path, skipTrash: Boolean = false): Boolean = {
        if (skipTrash)
            fs.delete(path, fs.isDirectory(path))
        else {
            Trash.moveToAppropriateTrash(fs, path, fs.getConf)
        }
    }

    /**
      * If the file at fileStatus is older than cutoffTimestampMs
      * and is empty, it will be deleted.
      *
      * @param cutoffTimestampMs  timestamp
      * @param skipTrash
      * @param fs                 FileSystem
      * @param fileStatus         FileStatus of file to check
      * @return                   1L if file is deleted, else 0L
      */
    def deleteIfOlderThanAndEmpty(
        cutoffTimestampMs: Long, skipTrash: Boolean = false
    )(
        fs: FileSystem, fileStatus: FileStatus
    ): Long = {
        val path = fileStatus.getPath

        if (fileStatus.getModificationTime < cutoffTimestampMs && isEmpty(fs, fileStatus)) {
            deleteOrTrash(fs, path, skipTrash) match {
                case true =>
                    log.debug(s"Deleted $path")
                    1L
                case false =>
                    log.warn(s"Failed deletion of $path")
                    0L
            }
        }
        else {
            log.debug(s"Not deleting $path, it is either not empty or not old enough")
            0L
        }
    }

    /**
      * @param cutoffTimestampMs
      * @param fs
      * @param fileStatus
      * @return 1 if fileStatus path is older than cutoffTimestampMs
      *         and is file or empty directory else 0
      */
    def isOlderThanAndEmpty(cutoffTimestampMs: Long)(
        fs: FileSystem, fileStatus: FileStatus
    ): Long = {
        if (fileStatus.getModificationTime < cutoffTimestampMs && isEmpty(fs, fileStatus))
            1L
        else
            0L
    }

    /**
      * Recursively deletes files and empty directories in path older than cutoffTimestampMs
      * @param fs
      * @param path
      * @param cutoffTimestampMs
      * @param skipTrash
      * @return number of files and directories deleted.
      */
    def deleteOlderThan(
        fs: FileSystem,
        path: Path,
        cutoffTimestampMs: Long,
        skipTrash: Boolean = false
    ): Long = {
        apply(fs, path, deleteIfOlderThanAndEmpty(cutoffTimestampMs, skipTrash))
    }

    /**
      * Counts files and empty directories older than than cutoffTimestampMs.
      * The returned count may reflect fewer directories than deleteOlderThan might
      * return, as directories that are emptied may be eligible for deletion
      * after their children are removed.
      * @param fs
      * @param path
      * @param cutoffTimestampMs
      * @return count of files and directories older than cutoffTimestampMs
      */
    def countOlderThan(fs: FileSystem, path: Path, cutoffTimestampMs: Long): Long = {
        apply(fs, path, isOlderThanAndEmpty(cutoffTimestampMs))
    }


    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = loadConfig(args)

        // If log4j is not configured with any appenders,
        // add a ConsoleAppender so logs are printed to the console.
        if (!log.getAllAppenders().hasMoreElements()) {
            addConsoleLogAppender()
        }

        val path: Path = new Path(config.path)
        val olderThanSeconds: Long = config.older_than_seconds
        val skipTrash: Boolean = config.skip_trash
        val dryRun: Boolean = config.dry_run
        val conf: Configuration = new Configuration

        try {
            val fs: FileSystem = path.getFileSystem(conf)
            val cutoffTimestampMs = System.currentTimeMillis - (olderThanSeconds * 1000L)

            if (!fs.exists(path)) {
                log.error(s"Cannot delete files in $path, it does not exist.")
                System.exit(1)
            }
            if (!dryRun) {
                log.info(
                    s"Deleting files older than $olderThanSeconds seconds in $path"
                )
                val deletedCount = deleteOlderThan(fs, path, cutoffTimestampMs, skipTrash)
                log.info(s"Deleted $deletedCount files and directories in $path (skipTrash=${skipTrash})")
            }
            else {
                log.info(
                    s"dry-run enabled: counting files older than $olderThanSeconds " +
                    s"seconds in $path"
                )
                val olderThanCount = countOlderThan(fs, path, cutoffTimestampMs)
                log.info(
                    s"dry-run enabled: Would have deleted $olderThanCount files and " +
                    s"directories older than $olderThanSeconds seconds in $path"
                )
            }
        }
        catch {
            case e: IOException =>
                log.error(
                    s"Failed deleting files older than $olderThanSeconds seconds in "  +
                    s"$path: ${e.getMessage}"
                )
                System.exit(1)
        }
        System.exit(0)
    }
}
