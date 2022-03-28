package org.wikimedia.analytics.refinery.job

import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}
import java.nio.file.{Paths, Files}
import java.nio.file.attribute.PosixFilePermission

class TestHDFSArchiver extends FlatSpec with Matchers {

    def runArchiverFailureTest(
        sourceDir: String,
        expectedFilenameEnding: String = ".gz",
        checkDone: Boolean = true
    ): Unit = {
        val archiveDir = "src/test/resources/hdfs_archiver_test/archive_dir"
        assert(!HDFSArchiver.apply(
            sourceDirectory = new Path(sourceDir),
            expectedFilenameEnding = expectedFilenameEnding,
            checkDone = checkDone,
            doneFilePath = new Path(Paths.get(sourceDir, "_SUCCESS").toString),
            archiveFile = new Path(Paths.get(archiveDir, "archive.gz").toString),
            archiveParentUmask = "022",
            archivePerms = "660"
        ))
    }

    it should "return false when the source dir does not exists" in {
        runArchiverFailureTest(sourceDir = "src/test/resources/hdfs_archiver_test/0")
    }

    it should "return false when the source is not a directory" in {
        runArchiverFailureTest(sourceDir = "src/test/resources/hdfs_archiver_test/a_file")
    }

    it should "return false when the done file is missing" in {
        runArchiverFailureTest(sourceDir = "src/test/resources/hdfs_archiver_test/1")
    }

    it should "return false if the source file is not here" in {
        runArchiverFailureTest(sourceDir = "src/test/resources/hdfs_archiver_test/2")
    }

    it should "return false if there are multiple sources" in {
        runArchiverFailureTest(sourceDir = "src/test/resources/hdfs_archiver_test/3")
    }

    it should "return false if there are multiple sources, without done file" in {
        runArchiverFailureTest(
            sourceDir = "src/test/resources/hdfs_archiver_test/4",
            checkDone = false
        )
    }

    it should "return false if the source is not matching the ending" in {
        runArchiverFailureTest(
            sourceDir = "src/test/resources/hdfs_archiver_test/5",
            checkDone = false
        )
    }

    it should "return false if the source file is 0B" in {
        runArchiverFailureTest(
            sourceDir = "src/test/resources/hdfs_archiver_test/6",
            checkDone = false
        )
    }

    it should "create an archive" in {
        val bkpDir = "src/test/resources/hdfs_archiver_test/7_bkp"
        val sourceDir = "src/test/resources/hdfs_archiver_test/7"
        val archiveDir = "src/test/resources/hdfs_archiver_test/7_archive"
        try {
            setupSource(bkpDir, sourceDir)
            val archiveFile = Paths.get(archiveDir, "archive.gz").toString
            assert(HDFSArchiver.apply(
                sourceDirectory = new Path(sourceDir),
                expectedFilenameEnding = ".gz",
                checkDone = true,
                doneFilePath = new Path(Paths.get(sourceDir, "_SUCCESS").toString),
                archiveFile = new Path(archiveFile),
                archiveParentUmask = "022",
                archivePerms = "660"
            ))
            assert(Files.exists(Paths.get(archiveFile)))
            val permissions = Files.getPosixFilePermissions(Paths.get(archiveFile))
            assert(permissions.contains(PosixFilePermission.OWNER_READ))
            assert(permissions.contains(PosixFilePermission.OWNER_WRITE))
            assert(permissions.contains(PosixFilePermission.GROUP_READ))
            assert(permissions.contains(PosixFilePermission.GROUP_WRITE))
            assert(!permissions.contains(PosixFilePermission.OTHERS_READ))
            // Permissions of archiveDir has not been tested because the test environment is not hdfs.
        } finally {
            cleanup(archiveDir, sourceDir.toString)
        }
    }

    def cleanup(archiveDir: String, sourceDir: String): Unit = {
        for (dir: String <- List(archiveDir, sourceDir)) {
            val dirPath = Paths.get(dir)
            if (Files.exists(dirPath)) {
                val files: List[AnyRef] = Files.list(dirPath).toArray.toList
                for (file <- files) {
                    Files.delete(Paths.get(file.toString))
                }
                Files.delete(dirPath)
            }
        }
    }

    def setupSource(bkpDir: String, sourceDir: String): Unit = {
        Files.createDirectory(Paths.get(sourceDir))
        //    Files.copy(Paths.get(bkpDir), Paths.get(sourceDir))
        Files.copy(Paths.get(bkpDir, "_SUCCESS"), Paths.get(sourceDir, "_SUCCESS"))
        Files.copy(Paths.get(bkpDir, "archive.gz"), Paths.get(sourceDir, "archive.gz"))
    }
}