package org.wikimedia.analytics.refinery.job

import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.core.config._

/**
 * Job to archive a file on HDFS.
 * The source file:
 * - ends with a specific string,
 * - is uniq but could be beside an empty flag file,
 * - and is not empty
 * The target is set with specific permissions.
 */
object HDFSArchiver extends LogHelper with ConfigHelper {

    /**
     * Config class for use config files and args.
     */
    case class Config(
        source_directory: String,
        archive_file: String,
        archive_parent_umask: String = "022",
        archive_perms: String = "644",
        expected_filename_ending: String = ".gz",
        check_done: Boolean = false,
        done_file: String = "_SUCCESS"
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
        val default = Config("", "")

        val propertiesDoc: ListMap[String, String] = ListMap(
            "source_directory" -> "Path of the directory where the source is located.",
            "archive_file" -> "Path of the archive file, where to put the source file.",
            "archive_parent_umask" ->
                s"""Umask for the archive directory permission.
                   | default: ${default.archive_parent_umask}""",
            "archive_perms" ->
                s"""Permissions given to the archive file.
                   | default: ${default.archive_perms}""",
            "expected_filename_ending" ->
                s"""The ending of the source file name.
                   |default: ${default.expected_filename_ending}""",
            "check_done" -> s"Check for a done file flag. default: ${default.check_done}",
            "done_file" -> s"Name of the done file flag. default: ${default.done_file}"
        )

        val usage: String =
            """
              |Job to archive a file on HDFS.
              |
              |Example:
              |  java -cp refinery-job.jar:$(/usr/bin/hadoop classpath) org.wikimedia.analytics.refinery.job.HDFSArchiver \
              |      --source_directory=/tmp/bob/source \
              |      --archive_file=/tmp/bob/public_archive\.gz
              |"""
    }

    /**
     * Entry point to run this job.
     *
     * @param args
     */
    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = loadConfig(args)

        // Make sure to log in the console when launched from Airflow
        addConsoleLogAppender()

        val statusCode: Int = if (apply(
            config.source_directory,
            config.expected_filename_ending,
            config.check_done,
            Path.mergePaths(config.source_directory, config.done_file),
            config.archive_file,
            config.archive_parent_umask,
            config.archive_perms
        )) 1 else 0
        System.exit(statusCode)
    }

    /**
     * The heart of the HDFS Archiver job
     *
     * @param sourceDirectory
     * @param expectedFilenameEnding
     * @param checkDone
     * @param doneFilePath
     * @param archiveFile
     * @param archiveParentUmask
     * @param archivePerms
     * @return boolean true in case of success
     */
    def apply(
        sourceDirectory: Path,
        expectedFilenameEnding: String,
        checkDone: Boolean,
        doneFilePath: Path,
        archiveFile: Path,
        archiveParentUmask: String,
        archivePerms: String
    ): Boolean = {
        val conf: Configuration = new Configuration
        conf.set("fs.permissions.umask-mode", archiveParentUmask)
        val fs: FileSystem = sourceDirectory.getFileSystem(conf)
        identifySourceFile(fs, sourceDirectory, expectedFilenameEnding, checkDone, doneFilePath) match {
            case None => false
            case Some(file) => createParentFolder(fs, archiveFile) &&
                archiveSource(fs, file, archiveFile, archivePerms)
        }
    }

    /**
     * identifySourceFile is validating that the source file is checking all conditions,
     * and if it does, will return the Path to the source file.
     * It checks:
     * - if the source directory exists
     * - if the check file exists (ex: _SUCCESS)
     * - if there is 1 not empty file in the directory beside the optional check file
     */
    def identifySourceFile(
        fs: FileSystem,
        sourceDirectory: Path,
        expectedFilenameEnding: String,
        checkDone: Boolean,
        doneFilePath: Path
    ): Option[Path] = {
        if (directoryExists(fs, sourceDirectory)) {
            if (checkDone && !fs.exists(doneFilePath)) {
                log.error(s"Done file ${doneFilePath.toString} is not present.")
                None
            } else {
                getSourceFileFromDirectory(fs, sourceDirectory, expectedFilenameEnding, checkDone)
            }
        } else {
            None
        }
    }

    /**
     * Checks if a specific path exists in the file system, and checks that it is a directory.
     */
    def directoryExists(fs: FileSystem, path: Path): Boolean = {
        if (!fs.exists(path)) {
            log.error(s"Dir ${path.toString} does not exist.")
            false
        } else if (!fs.isDirectory(path)) {
            log.error(s"Dir ${path.toString} is not a directory.")
            false
        } else {
            true
        }
    }

    /**
     * Try to get the Path to the source file and checks that a there is single non empty file in the source directory
     * (except the success file).
     */
    def getSourceFileFromDirectory(
        fs: FileSystem,
        sourceDirectory: Path,
        expectedFilenameEnding: String,
        checkDone: Boolean
    ): Option[Path] = {
        val files = listFilesInDir(fs, sourceDirectory)
        if ((checkDone && files.length != 2) || (!checkDone && files.length != 1)) {
            log.error(s"Wrong file count in ${sourceDirectory.toString}")
            None
        } else {
            val sourceFile: Option[LocatedFileStatus] = files
                .find {
                    _.getPath.getName.endsWith(expectedFilenameEnding)
                }
            if (sourceFile.isEmpty) {
                log.error(s"Missing source in ${sourceDirectory.toString} (ending in: $expectedFilenameEnding)")
                None
            } else if (sourceFile.get.getLen == 0) {
                log.error(s"Empty source in ${sourceDirectory.toString} (ending in: $expectedFilenameEnding)")
                None
            } else {
                Some(sourceFile.get.getPath)
            }
        }
    }

    /**
     * Lists the files into a directory, and converts the iterator into a list for convenience.
     */
    def listFilesInDir(fs: FileSystem, dir: Path): List[LocatedFileStatus] = {
        val result = ListBuffer[LocatedFileStatus]()
        val iterator = fs.listFiles(dir, true)
        while (iterator.hasNext) result += iterator.next
        result.toList
    }

    def createParentFolder(fs: FileSystem, file: Path): Boolean = {
        // This import has to happen after setting the umask mode
        import org.apache.hadoop.fs.permission.FsPermission
        fs.mkdirs(file.getParent, new FsPermission("777")) // Only restrict through umask.
    }

    def archiveSource(fs: FileSystem, sourceFile: Path, archiveFile: Path, archivePerms: String): Boolean = {
        import org.apache.hadoop.fs.permission.FsPermission
        fs.delete(archiveFile, false)
        val result = fs.rename(sourceFile, archiveFile) &&
            fs.delete(sourceFile.getParent, true) &&
            fs.setPermission(archiveFile, new FsPermission(archivePerms)).equals() // "equals" is here to get a bool return.
        if (result) {
            log.info(s"Archive created: ${archiveFile.toString}")
        }
        result
    }
}