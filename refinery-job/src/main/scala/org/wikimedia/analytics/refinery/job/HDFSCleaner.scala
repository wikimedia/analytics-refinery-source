package org.wikimedia.analytics.refinery.job

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.wikimedia.analytics.refinery.core.LogHelper


/**
  * Aids in deleting files older than a given mtime.
  * Useful for cleaning the HDFS tmp directory in HDFS.
  */
object HDFSCleaner extends LogHelper {
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
        val iterator = fs.listLocatedStatus(path)

        var appliedCount = 0L
        while (iterator.hasNext) {
            val nextFile = iterator.next

            // If recursing and we've got a directory, recurse into it and apply callback
            // to all enclosed files and directories before moving on.
            if (recursive && nextFile.isDirectory) {
                appliedCount = appliedCount + apply(fs, nextFile.getPath, callback)
            }
            // Apply callback to current file or directory
            appliedCount = appliedCount + callback(fs, nextFile)
        }
        appliedCount
    }

    /**
      * True if path does not contain any files or directories.
      * Files are always considered empty paths.
      * @param fs
      * @param path
      * @return
      */
    def isEmpty(fs: FileSystem, path: Path): Boolean = {
        fs.isFile(path) || !fs.listFiles(path, false).hasNext
    }

    /**
      * If the file at fileStatus is older than cutoffTimestampMs
      * and is empty, it will be deleted.
      *
      * @param cutoffTimestampMs  timestamp
      * @param fs                 FileSystem
      * @param fileStatus         FileStatus of file to check
      * @return                   1L if file is deleted, else 0L
      */
    def deleteIfOlderThanAndEmpty(cutoffTimestampMs: Long)(
        fs: FileSystem, fileStatus: FileStatus
    ): Long = {
        val path = fileStatus.getPath
        if (fileStatus.getModificationTime < cutoffTimestampMs && isEmpty(fs, path)) {
            fs.delete(path, fs.isDirectory(path)) match {
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
        if (fileStatus.getModificationTime < cutoffTimestampMs && isEmpty(fs, fileStatus.getPath))
            1L
        else
            0L
    }

    /**
      * Recursively deletes files and empty directories in path older than cutoffTimestampMs
      * @param fs
      * @param path
      * @param cutoffTimestampMs
      * @return number of files and directories deleted.
      */
    def deleteOlderThan(fs: FileSystem, path: Path, cutoffTimestampMs: Long): Long = {
        apply(fs, path, deleteIfOlderThanAndEmpty(cutoffTimestampMs))
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
        if (args == null || (args.length != 2 && args.length != 3)) {
            System.err.println(
                "Usage: HDFSCleaner <path> <older_than_seconds> [--dry-run]\n\n" +
                "Finds files in <path> with mtimes <older_than_seconds> and deletes them."
            )
            System.exit(1)
        }

        // If log4j is not configured with any appenders,
        // add a ConsoleAppender so logs are printed to the console.
        if (!log.getAllAppenders().hasMoreElements()) {
            addConsoleLogAppender()
        }

        val path: Path = new Path(args(0))
        val olderThanSeconds: Long = args(1).toLong
        val dryRun: Boolean = args.length == 3 && (args(2) == "--dry-run" || args(2) == "-n")
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
                val deletedCount = deleteOlderThan(fs, path, cutoffTimestampMs)
                log.info(s"Deleted $deletedCount files and directories in $path")
            }
            else {
                log.info(
                    s"dry-run enabled: counting files older than $olderThanSeconds " +
                    s"seconds in $path"
                )
                val olderThanCount = countOlderThan(fs, path, cutoffTimestampMs)
                log.info(
                    s"dry-run enabled: Would have deleted $olderThanCount files and " +
                    s"directories older than $olderThanSeconds in $path"
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
