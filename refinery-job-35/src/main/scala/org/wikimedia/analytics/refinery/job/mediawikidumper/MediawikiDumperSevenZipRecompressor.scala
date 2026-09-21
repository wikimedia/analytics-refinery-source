package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.io.{
    BufferedInputStream,
    File,
    FileInputStream,
    IOException,
    ObjectInputStream,
    ObjectOutputStream,
    PrintWriter
}
import java.security.{DigestInputStream, MessageDigest}
import java.util.Collections

import org.apache.commons.compress.archivers.sevenz.{
    SevenZArchiveEntry,
    SevenZFile,
    SevenZMethod,
    SevenZMethodConfiguration,
    SevenZOutputFile
}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.tukaani.xz.LZMA2Options
import scopt.OptionParser

/** Job to recompress the bzip2 output of [[MediawikiDumper]] as 7z.
  *
  * The v1 dump pipeline offers the full history content as both 7z and bz2. The
  * 7z files are about 60% of the history downloads. This job gives v2 the same
  * alternative. See T437454.
  *
  * The job maps one input file to one output file, the same as the v1
  * `XmlRecompressDump` stage. It does not generate XML again. A second
  * [[MediawikiDumper]] run would repeat the revision content shuffle, which is
  * the most expensive stage of that job. This job reads only the finished bz2
  * files instead.
  *
  * v2 puts each compression algorithm in its own directory, so the input and the
  * output folder differ:
  *   - in: `.../{wiki}/{date}/xml/bzip2/{wiki}-{date}-{pageRange}.xml.bz2`
  *   - out: `.../{wiki}/{date}/xml/7z/{wiki}-{date}-{pageRange}.xml.7z`
  *
  * For efficiency in Airflow, this job does many recompression related tasks:
  * it applies a deny list, clears its own output folder, recompresses the files,
  * and writes the SHA256SUMS manifest.
  *
  * Each task is single-threaded on purpose. The 7-Zip `-mmt` option splits the
  * input into blocks and resets the dictionary at every block, which cancels the
  * gain from a large dictionary. The XZ for Java encoder cannot make this
  * mistake. Parallelism comes from Spark tasks.
  *
  * The default settings come from measured runs on simplewiki. They give output
  * about 20% smaller than v1, at about 1,200 core hours for each TiB of bz2
  * input. The gain over v1 comes from the BT4 match finder, and not from a large
  * dictionary. So the job keeps the 4 MiB dictionary of v1, which holds the
  * cluster cost down and keeps the memory a reader needs unchanged.
  */
object MediawikiDumperSevenZipRecompressor {

    // This module targets Spark 3.5, which ships log4j 2 with the log4j-1.2-api bridge.
    // Do not use refinery-tools LogHelper here. It sets the level through the bridge, and
    // it would pull the whole refinery-tools dependency tree into this module.
    @transient lazy val log: Logger = Logger.getLogger(this.getClass)

    lazy val spark: SparkSession = SparkSession.builder.getOrCreate

    /** Size of the copy and digest buffers, in bytes. */
    private val IOBufferBytes: Int = 256 * 1024

    private val SevenZipSuffix: String = ".7z"

    /** Suffix of the hidden file that holds an upload in progress. */
    private val InProgressSuffix: String = ".inprogress"

    /** Name of the checksum manifest. The bzip2 export uses the same name. */
    private val ChecksumManifestName: String = "SHA256SUMS"

    /** The result of one file, used to report the job totals.
      *
      * @param name
      *   the output file name
      * @param compressedInputBytes
      *   size of the bz2 input
      * @param uncompressedBytes
      *   size of the XML between the two codecs
      * @param compressedOutputBytes
      *   size of the 7z output
      * @param elapsedMs
      *   wall time of the task
      * @param sevenZipSha256
      *   SHA-256 digest of the 7z output, as lower case hex. The job writes the
      *   SHA256SUMS manifest from these values.
      */
    case class RecompressResult(
        name: String,
        compressedInputBytes: Long,
        uncompressedBytes: Long,
        compressedOutputBytes: Long,
        elapsedMs: Long,
        sevenZipSha256: String
    )

    /** Dictionary sizes that 7z can store, in bytes.
      *
      * 7z holds the LZMA2 dictionary size in a single property byte. That byte
      * can express only `2^n` and `3 * 2^(n-1)`. Commons Compress does not check
      * the value, and `LZMA2Options.setDictSize` does not either. Both accept any
      * size between the minimum and the maximum. Commons Compress then writes a
      * wrong property byte, and other tools read a wrong dictionary size. So
      * check the value before the job starts.
      */
    lazy val validDictionarySizes: Seq[Long] = {
        (12 to 31)
            .flatMap(exponent => Seq(1L << exponent, 3L << (exponent - 1)))
            .filter(size =>
                size >= LZMA2Options.DICT_SIZE_MIN &&
                    size <= LZMA2Options.DICT_SIZE_MAX
            )
            .distinct
            .sorted
    }

    /** Main class entry point.
      *
      * @param params
      *   parsed Params including the input and the output folder.
      * @return
      *   Unit
      */
    def apply(params: Params): Unit = {
        validate(params)

        // Since deny lists will typically be small, we apply it here instead of as a task in Airflow.
        if (params.denyList.contains(params.wikiId)) {
            log.info(
              s"${params.wikiId} is on the deny list. This wiki gets no 7z copy."
            )
            return
        }

        val hadoopConf = spark.sparkContext.hadoopConfiguration
        val inputFolder = new Path(params.inputFolder)
        val outputFolder = new Path(params.outputFolder)
        val fs = FileSystem.get(inputFolder.toUri, hadoopConf)

        val options = buildLZMA2Options(params)
        val encoderHeapMB = options.getEncoderMemoryUsage / 1024
        val decoderHeapMB = options.getDecoderMemoryUsage / 1024
        log.info(s"""Mediawiki Dumper 7z recompressor
             | from ${params.inputFolder}
             | to ${params.outputFolder}
             | dictionary: ${params.dictionarySizeMB} MiB
             | encoder heap per task: about $encoderHeapMB MiB
             | decoder heap for a reader: about $decoderHeapMB MiB"""
            .stripMargin)

        // Remove the output of an earlier run. A rerun can write a different set of
        // file names, because the export decides the page ranges. A stale file would
        // stay in the folder, and the SHA256SUMS manifest would not list it.
        if (params.clearOutputFolder && fs.exists(outputFolder)) {
            log.info(s"Clearing ${params.outputFolder}")
            fs.delete(outputFolder, true)
        }

        if (!fs.exists(outputFolder)) {
            fs.mkdirs(outputFolder)
        }

        // buildWorkList fails when the input folder holds no dump file, so the list
        // always holds at least one pair here.
        val work = buildWorkList(fs, inputFolder, outputFolder, params)
        val serializableConf = new SerializableHadoopConfiguration(hadoopConf)

        // One file per task. Spark then retries one file, not a batch.
        // A task can run for hours, so this keeps the cost of a retry low.
        val results = spark
            .sparkContext
            .parallelize(work, work.size)
            .map { case (inputPathString, outputPathString) =>
                recompressOneFile(
                  new Path(inputPathString),
                  new Path(outputPathString),
                  params,
                  serializableConf
                )
            }
            .collect()

        if (params.writeChecksums) {
            writeChecksumManifest(fs, outputFolder, results)
        }

        logSummary(results)

        log.info(
          s"Mediawiki Dumper 7z recompressor: Done. Output in ${params.outputFolder}"
        )
    }

    /** Checks the parameters before the job starts.
      *
      * @param params
      *   the job parameters
      */
    def validate(params: Params): Unit = {
        val dictionaryBytes = params.dictionarySizeMB.toLong * 1024 * 1024

        if (!validDictionarySizes.contains(dictionaryBytes)) {
            // The table also holds sizes that are not a whole number of MiB, for
            // example 1.5 MiB. This option takes MiB, so list the whole ones only.
            val validMB = validDictionarySizes
                .filter(size => size >= 1024 * 1024 && size % (1024 * 1024) == 0)
                .map(_ / (1024 * 1024))
                .mkString(", ")
            throw new IllegalArgumentException(
              s"A dictionary of ${params.dictionarySizeMB} MiB is not a size that 7z can store. " +
                  s"Use one of these sizes in MiB: $validMB"
            )
        }

        if (params.niceLength < 8 || params.niceLength > 273) {
            throw new IllegalArgumentException(
              s"nice_length must be between 8 and 273, got ${params.niceLength}"
            )
        }

        if (params.positionBits < 0 || params.positionBits > 4) {
            throw new IllegalArgumentException(
              s"position_bits must be between 0 and 4, got ${params.positionBits}"
            )
        }

        if (params.inputExtension == params.outputExtension) {
            throw new IllegalArgumentException(
              "input_extension and output_extension must differ"
            )
        }

        // A deny list without a wiki id never matches, so the job would run for a
        // wiki that must get no 7z copy. Fail instead of doing the wrong work.
        if (params.denyList.nonEmpty && params.wikiId.isEmpty) {
            throw new IllegalArgumentException(
              "deny_list needs wiki_id. Pass the wiki that this run recompresses."
            )
        }

        // The manifest lists the files of this run. Without a clear, the folder can
        // hold a file from an earlier run, and the manifest would not list it.
        if (params.writeChecksums && !params.clearOutputFolder) {
            throw new IllegalArgumentException(
              "write_checksums needs clear_output_folder. " +
                  "The manifest lists the files of this run only."
            )
        }

        if (!params.outputExtension.endsWith(SevenZipSuffix)) {
            throw new IllegalArgumentException(
              s"output_extension must end with $SevenZipSuffix, got ${params.outputExtension}"
            )
        }
    }

    /** Builds the encoder settings.
      *
      * `LZMA2Options` is not serializable, so every task calls this method.
      *
      * @param params
      *   the job parameters
      * @return
      *   the encoder settings
      */
    def buildLZMA2Options(params: Params): LZMA2Options = {
        val options = new LZMA2Options()
        // Set the preset first. It resets every other field.
        options.setPreset(9)
        options.setDictSize(params.dictionarySizeMB * 1024 * 1024)
        options.setMatchFinder(LZMA2Options.MF_BT4)
        options.setNiceLen(params.niceLength)
        // 0 means the encoder picks the depth from the match finder and the nice
        // length. A fixed high value makes BT4 much slower for little gain.
        options.setDepthLimit(params.depthLimit)
        // MediaWiki XML has no byte alignment pattern. The default of 2 position
        // bits wastes bits on this data.
        options.setPb(params.positionBits)
        options
    }

    /** Pairs each input file with its output file.
      *
      * @param fs
      *   the file system of the input folder
      * @param inputFolder
      *   folder holding the bz2 files
      * @param outputFolder
      *   folder to hold the 7z files
      * @param params
      *   the job parameters
      * @return
      *   pairs of input path and output path, largest input first
      */
    def buildWorkList(
        fs: FileSystem,
        inputFolder: Path,
        outputFolder: Path,
        params: Params
    ): Seq[(String, String)] = {
        val inputs = listInputFiles(fs, inputFolder, params.inputExtension)

        if (inputs.isEmpty) {
            throw new IllegalStateException(
              s"Found no *${params.inputExtension} file in $inputFolder"
            )
        }

        inputs
            .map { status =>
                val outputName = status
                    .getPath
                    .getName
                    .stripSuffix(params.inputExtension) + params.outputExtension
                (status, new Path(outputFolder, outputName))
            }
            // Largest input first, so a slow file does not start in the last wave of
            // tasks and run alone.
            //
            // The gain is small. max_partition_size caps the uncompressed content, so
            // every file holds about the same amount of XML, and measured times span
            // only about 40%. A larger bz2 for the same XML means less compressible
            // content, which does take the encoder longer, but the measured link is
            // not consistent. Keep the sort as cheap insurance, not as a tuning.
            .sortBy { case (status, _) => -status.getLen }
            .map { case (status, outputPath) =>
                (status.getPath.toString, outputPath.toString)
            }
    }

    /** Lists the dump files in a folder.
      *
      * @param fs
      *   the file system of the folder
      * @param folder
      *   the folder to list
      * @param extension
      *   the extension of the dump files
      * @return
      *   the matching file statuses, ordered by name
      */
    def listInputFiles(
        fs: FileSystem,
        folder: Path,
        extension: String
    ): Seq[FileStatus] = {
        fs.listStatus(folder)
            .filter(_.isFile)
            .filter(_.getPath.getName.endsWith(extension))
            // Ignore hidden files and files like _SUCCESS.
            .filterNot(_.getPath.getName.startsWith("_"))
            .filterNot(_.getPath.getName.startsWith("."))
            .sortBy(_.getPath.getName)
            .toSeq
    }

    /** Recompresses one bzip2 file as 7z.
      *
      * The 7z format writes its start header at offset 0 after it writes the
      * body, so the writer must seek back to the start. HDFS does not support
      * seek on write. `FSDataOutputStream` only appends. So the method writes the
      * archive to container scratch space first, then uploads it.
      *
      * The method uploads to a hidden name and renames it. Rename is one metadata
      * operation on HDFS, so a reader never sees a partial file.
      *
      * The upload also builds the SHA-256 digest of the archive. The job writes the
      * SHA256SUMS manifest from these digests, and reads no published file again.
      *
      * @param inputPath
      *   the bz2 file to read
      * @param outputPath
      *   the 7z file to write
      * @param params
      *   the job parameters
      * @param serializableConf
      *   the Hadoop configuration of the driver
      * @return
      *   the sizes and the wall time of this file
      */
    def recompressOneFile(
        inputPath: Path,
        outputPath: Path,
        params: Params,
        serializableConf: SerializableHadoopConfiguration
    ): RecompressResult = {
        val startMs = System.currentTimeMillis()
        val conf = serializableConf.value
        val fs = FileSystem.get(outputPath.toUri, conf)
        val compressedInputBytes = fs.getFileStatus(inputPath).getLen

        // The archive holds one entry. v1 pipes stdin into 7-Zip, which names the
        // entry after the archive with '.7z' removed. Use the same name, so
        // '7z x' gives a reader the same file name as v1 does.
        val entryName = outputPath.getName.stripSuffix(SevenZipSuffix)

        val stagingPath = new Path(
          outputPath.getParent,
          s".${outputPath.getName}$InProgressSuffix"
        )

        val partitionId = Option(TaskContext.get()).map(_.partitionId).getOrElse(0)
        val tempArchive = File.createTempFile(
          "mediawikidumper-",
          SevenZipSuffix,
          localScratchDir(partitionId)
        )

        try {
            val (uncompressedBytes, sourceDigest) = writeArchive(
              fs,
              inputPath,
              tempArchive,
              entryName,
              params
            )

            if (params.verify) {
                verifyArchive(tempArchive, entryName, sourceDigest)
            }

            // The upload reads every byte of the archive, so it also builds the
            // SHA-256 that the SHA256SUMS manifest needs. A separate job that reads
            // the published files again costs a full serial read of the output.
            val sevenZipSha256 = uploadAndDigest(fs, tempArchive, stagingPath)

            // Rename fails when the target exists, which happens when
            // clear_output_folder is off and the job reruns.
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, false)
            }

            if (!fs.rename(stagingPath, outputPath)) {
                throw new IOException(
                  s"Failed to rename $stagingPath to $outputPath"
                )
            }

            val compressedOutputBytes = fs.getFileStatus(outputPath).getLen
            val elapsedMs = System.currentTimeMillis() - startMs

            log.info(
              s"Recompressed ${outputPath.getName}: " +
                  s"${compressedInputBytes / (1024 * 1024)} MiB bz2, " +
                  s"${uncompressedBytes / (1024 * 1024)} MiB xml, " +
                  s"${compressedOutputBytes / (1024 * 1024)} MiB 7z, " +
                  s"${elapsedMs / 1000} s"
            )

            RecompressResult(
              name = outputPath.getName,
              compressedInputBytes = compressedInputBytes,
              uncompressedBytes = uncompressedBytes,
              compressedOutputBytes = compressedOutputBytes,
              elapsedMs = elapsedMs,
              sevenZipSha256 = sevenZipSha256
            )
        } finally {
            if (tempArchive.exists() && !tempArchive.delete()) {
                log.warn(
                  s"Failed to delete the temporary file ${tempArchive.getAbsolutePath}"
                )
            }
            // Remove the staging file, so a failed attempt leaves nothing behind.
            try {
                if (fs.exists(stagingPath)) {
                    fs.delete(stagingPath, false)
                }
            } catch {
                case e: IOException =>
                    log.warn(s"Failed to delete the staging file $stagingPath", e)
            }
        }
    }

    /** Decodes the bz2 input and writes the 7z archive to local disk.
      *
      * The method streams the data. Only the compressed bytes reach local disk.
      *
      * @param fs
      *   the file system of the input file
      * @param inputPath
      *   the bz2 file to read
      * @param tempArchive
      *   the local file to write
      * @param entryName
      *   the name of the single entry in the archive
      * @param params
      *   the job parameters
      * @return
      *   the number of XML bytes, and their SHA-256 digest
      */
    private def writeArchive(
        fs: FileSystem,
        inputPath: Path,
        tempArchive: File,
        entryName: String,
        params: Params
    ): (Long, Array[Byte]) = {
        val digest = MessageDigest.getInstance("SHA-256")
        val sevenZOutput = new SevenZOutputFile(tempArchive)

        try {
            sevenZOutput.setContentMethods(
              Collections.singletonList(
                new SevenZMethodConfiguration(
                  SevenZMethod.LZMA2,
                  buildLZMA2Options(params)
                )
              )
            )

            val entry = new SevenZArchiveEntry()
            entry.setName(entryName)
            entry.setDirectory(false)
            // Do not set a modification date. v1 stores none either, and an
            // absent date keeps the output repeatable byte for byte. The
            // SHA256SUMS manifest of the output folder needs that.
            sevenZOutput.putArchiveEntry(entry)

            val input = new BZip2CompressorInputStream(
              new BufferedInputStream(fs.open(inputPath), IOBufferBytes),
              // Read concatenated bzip2 streams. The dumper writes one stream per
              // file today. This flag keeps the reader correct if that changes.
              true
            )

            val uncompressedBytes =
                try {
                    val buffer = new Array[Byte](IOBufferBytes)
                    var total = 0L
                    var read = input.read(buffer)
                    while (read != -1) {
                        sevenZOutput.write(buffer, 0, read)
                        digest.update(buffer, 0, read)
                        total += read
                        read = input.read(buffer)
                    }
                    total
                } finally {
                    input.close()
                }

            sevenZOutput.closeArchiveEntry()
            (uncompressedBytes, digest.digest())
        } finally {
            sevenZOutput.close()
        }
    }

    /** Uploads the local archive and returns its SHA-256 digest.
      *
      * The method streams the file, so it holds only one buffer in memory. The
      * digest costs almost nothing, because the upload reads every byte anyway.
      *
      * @param fs
      *   the file system of the target
      * @param tempArchive
      *   the local 7z file to upload
      * @param targetPath
      *   the path to write
      * @return
      *   the digest of the archive, as lower case hex
      */
    private def uploadAndDigest(
        fs: FileSystem,
        tempArchive: File,
        targetPath: Path
    ): String = {
        val digest = MessageDigest.getInstance("SHA-256")
        val input = new DigestInputStream(
          new BufferedInputStream(new FileInputStream(tempArchive), IOBufferBytes),
          digest
        )

        try {
            // Overwrite a staging file that a failed attempt left behind.
            val output = fs.create(targetPath, true)

            try {
                val buffer = new Array[Byte](IOBufferBytes)
                var read = input.read(buffer)
                while (read != -1) {
                    output.write(buffer, 0, read)
                    read = input.read(buffer)
                }
            } finally {
                output.close()
            }
        } finally {
            input.close()
        }

        toHex(digest.digest())
    }

    /** Writes the SHA256SUMS manifest of the output folder.
      *
      * The format and the order match [[HdfsFileFingerprintWriter]], which the
      * bzip2 export uses, so `sha256sum -c SHA256SUMS` behaves the same for both
      * formats.
      *
      * The digests come from the tasks. The job does not read the published files
      * again.
      *
      * @param fs
      *   the file system of the output folder
      * @param outputFolder
      *   the folder that holds the 7z files
      * @param results
      *   the result of every file of this run
      */
    def writeChecksumManifest(
        fs: FileSystem,
        outputFolder: Path,
        results: Seq[RecompressResult]
    ): Unit = {
        val manifestPath = new Path(outputFolder, ChecksumManifestName)
        val writer = new PrintWriter(fs.create(manifestPath, true))

        try {
            results
                .sortBy(_.name)
                .foreach(result =>
                    writer.println(s"${result.sevenZipSha256}  ${result.name}")
                )
        } finally {
            writer.close()
        }

        log.info(
          s"Wrote $manifestPath with ${results.size} entries."
        )
    }

    /** Reads the archive back and compares it with the source.
      *
      * This proves that the published 7z file holds the same XML as the bz2 file for
      * the same page range.
      *
      * The check is not cheap. A measured run adds 35% to the core time, because the
      * decode competes with the encoders of the other tasks on the same executor. So
      * the check is off by default.
      *
      * @param archive
      *   the local 7z file to read
      * @param entryName
      *   the expected name of the single entry
      * @param expectedDigest
      *   the SHA-256 digest of the source XML
      */
    private def verifyArchive(
        archive: File,
        entryName: String,
        expectedDigest: Array[Byte]
    ): Unit = {
        val digest = MessageDigest.getInstance("SHA-256")
        val sevenZFile = new SevenZFile(archive)

        try {
            val entry = sevenZFile.getNextEntry

            if (entry == null) {
                throw new IOException(s"$archive holds no entry")
            }

            if (entry.getName != entryName) {
                throw new IOException(
                  s"$archive holds the entry '${entry.getName}', expected '$entryName'"
                )
            }

            val buffer = new Array[Byte](IOBufferBytes)
            var read = sevenZFile.read(buffer)
            while (read != -1) {
                digest.update(buffer, 0, read)
                read = sevenZFile.read(buffer)
            }

            if (sevenZFile.getNextEntry != null) {
                throw new IOException(s"$archive holds more than one entry")
            }
        } finally {
            sevenZFile.close()
        }

        val actualDigest = digest.digest()

        if (!MessageDigest.isEqual(expectedDigest, actualDigest)) {
            throw new IOException(
              s"$archive does not hold the source XML. " +
                  s"Expected SHA-256 ${toHex(expectedDigest)}, got ${toHex(actualDigest)}"
            )
        }
    }

    /** Formats a digest for a log message.
      *
      * @param bytes
      *   the digest
      * @return
      *   the digest as lower case hex
      */
    private def toHex(bytes: Array[Byte]): String = {
        bytes.map(byte => f"${byte & 0xff}%02x").mkString
    }

    /** Returns a scratch directory for the running container.
      *
      * YARN sets `LOCAL_DIRS` from `yarn.nodemanager.local-dirs`, and removes
      * these directories after the container stops. Do not use `/tmp`. `/tmp` is
      * often small, and YARN does not clean it after a container dies.
      *
      * @param partitionId
      *   the partition of the running task, used to spread the load over the
      *   configured disks
      * @return
      *   the directory to hold the temporary archive
      */
    def localScratchDir(partitionId: Int): File = {
        val localDirs = sys
            .env
            .get("LOCAL_DIRS")
            .map(_.split(",").map(_.trim).filter(_.nonEmpty))
            .getOrElse(Array.empty[String])

        if (localDirs.isEmpty) {
            val fallback = System.getProperty("java.io.tmpdir")
            log.warn(
              s"LOCAL_DIRS is not set. Using $fallback. Expect this outside YARN only."
            )
            new File(fallback)
        } else {
            new File(localDirs(Math.floorMod(partitionId, localDirs.length)))
        }
    }

    /** Logs the job totals.
      *
      * The rate is the measure that a cluster cost estimate needs. Every task
      * uses one core, so this is a per-core rate.
      *
      * @param results
      *   the result of every file
      */
    def logSummary(results: Seq[RecompressResult]): Unit = {
        if (results.isEmpty) {
            log.info("7z recompression totals: no file was recompressed.")
            return
        }

        val megabyte = 1024.0 * 1024.0
        val fileCount = results.size
        val inputMB = results.map(_.compressedInputBytes).sum / megabyte
        val xmlMB = results.map(_.uncompressedBytes).sum / megabyte
        val outputMB = results.map(_.compressedOutputBytes).sum / megabyte
        val totalMs = results.map(_.elapsedMs).sum
        val coreHours = totalMs / 3600000.0
        val slowest = results.maxBy(_.elapsedMs)
        val slowestSeconds = slowest.elapsedMs / 1000

        val rate = if (totalMs > 0) xmlMB / (totalMs / 1000.0) else 0.0
        val percentOfBz2 = if (inputMB > 0) outputMB / inputMB * 100 else 0.0

        log.info(f"""7z recompression totals
             | files: $fileCount
             | bz2 in: $inputMB%.1f MiB
             | xml: $xmlMB%.1f MiB
             | 7z out: $outputMB%.1f MiB
             | 7z size against bz2: $percentOfBz2%.1f%%
             | core time: $coreHours%.2f h
             | rate: $rate%.2f MiB/s per core
             | slowest file: ${slowest.name}%s at $slowestSeconds%d s"""
            .stripMargin)
    }

    case class Params(
        inputFolder: String = "",
        outputFolder: String = "",
        inputExtension: String = ".xml.bz2",
        outputExtension: String = ".xml.7z",
        dictionarySizeMB: Int = 4,
        niceLength: Int = 64,
        positionBits: Int = 0,
        depthLimit: Int = 0,
        verify: Boolean = false,
        wikiId: String = "",
        denyList: Seq[String] = Seq.empty,
        clearOutputFolder: Boolean = true,
        writeChecksums: Boolean = true
    )

    /** Define the command line options parser
      */
    val argsParser: OptionParser[Params] = {
        new OptionParser[Params]("Mediawiki XML Dumper 7z recompressor job") {
            head("Mediawiki XML dumper 7z recompressor job", "")
            note(
              "This job recompresses the bzip2 output of the Mediawiki XML dumper as 7z."
            )
            help("help") text "Prints this usage text"

            opt[String]('i', "input_folder") required
                () valueName "<input_folder>" action { (x, p) =>
                    p.copy(inputFolder = x)
                } text "The folder holding the bzip2 dump files."

            opt[String]('o', "output_folder") required
                () valueName "<output_folder>" action { (x, p) =>
                    p.copy(outputFolder = x)
                } text "The folder to hold the 7z dump files."

            opt[String]("input_extension") optional
                () valueName "<input_extension>" action { (x, p) =>
                    p.copy(inputExtension = x)
                } text "The extension of the input files. Defaults to .xml.bz2"

            opt[String]("output_extension") optional
                () valueName "<output_extension>" action { (x, p) =>
                    p.copy(outputExtension = x)
                } text "The extension of the output files. Defaults to .xml.7z"

            opt[Int]('d', "dictionary_size") optional
                () valueName "<dictionary_size>" action { (x, p) =>
                    p.copy(dictionarySizeMB = x)
                } text
                """The LZMA2 dictionary size in MiB. Defaults to 4, the size that v1 uses.
                |A measured sweep from 4 MiB to 128 MiB on simplewiki shows no knee. Every
                |doubling gives about 1.5% less size, and 32 times the dictionary gives only
                |8% less size. The match finder, not the dictionary, is what makes this job
                |beat v1. So we keep the v1 size, and the cluster cost stays low.
                |Keeping the v1 size also keeps the memory a reader needs unchanged. That
                |matters, because 7-Zip fails when it cannot allocate the dictionary.
                |Note that the sweep used one small wiki. A page with a long history holds
                |more redundancy than a dictionary of this size spans, so a large wiki may
                |gain more. See T437454.
                |7z stores this size in one byte, so only some sizes are valid. The job
                |lists them when the value is wrong.
                |The encoder needs about 10.5 times this size of heap, for every task that
                |runs at the same time."""
                    .stripMargin

            opt[Int]("nice_length") optional
                () valueName "<nice_length>" action { (x, p) =>
                    p.copy(niceLength = x)
                } text
                """The LZMA2 nice match length, between 8 and 273. Defaults to 64.
                |64 is the LZMA2 preset 9 default. A measured run on simplewiki shows that 273
                |makes the job 1.66 times slower and the output only 5% smaller. A larger
                |dictionary buys about 33 times more size per core hour. See T437454."""
                    .stripMargin

            opt[Int]("position_bits") optional
                () valueName "<position_bits>" action { (x, p) =>
                    p.copy(positionBits = x)
                } text
                """The LZMA2 position bits, between 0 and 4. Defaults to 0.
                |XML has no byte alignment pattern, so the LZMA2 default of 2 wastes bits."""
                    .stripMargin

            opt[Int]("depth_limit") optional
                () valueName "<depth_limit>" action { (x, p) =>
                    p.copy(depthLimit = x)
                } text
                """The LZMA2 match finder depth limit. Defaults to 0.
                |0 lets the encoder pick the depth. A high value makes the job much slower
                |for little gain."""
                    .stripMargin

            opt[String]("wiki_id") optional
                () valueName "<wiki_id>" action { (x, p) =>
                    p.copy(wikiId = x)
                } text
                """The wiki that this run recompresses. Only deny_list reads this.
                |Set it together with deny_list."""
                    .stripMargin

            opt[Seq[String]]("deny_list") optional
                () valueName "<wiki1,wiki2>" action { (x, p) =>
                    p.copy(denyList = x)
                } text
                """Wikis that get no 7z copy. Empty by default.
                |The job does nothing when wiki_id is on this list."""
                    .stripMargin

            opt[Boolean]("clear_output_folder") optional
                () valueName "<clear_output_folder>" action { (x, p) =>
                    p.copy(clearOutputFolder = x)
                } text
                """Delete the output folder before the run. Defaults to true.
                |A rerun can write a different set of file names, because the export decides
                |the page ranges. A stale file would stay in the folder, and SHA256SUMS would
                |not list it."""
                    .stripMargin

            opt[Boolean]("write_checksums") optional
                () valueName "<write_checksums>" action { (x, p) =>
                    p.copy(writeChecksums = x)
                } text
                """Write the SHA256SUMS manifest of the output folder. Defaults to true.
                |The tasks give the digests, so this costs nothing. It needs
                |clear_output_folder, because the manifest lists the files of this run only."""
                    .stripMargin

            opt[Boolean]("verify") optional
                () valueName "<verify>" action { (x, p) =>
                    p.copy(verify = x)
                } text
                """Read each archive back and compare it with the source. Defaults to false.
                |A measured run on simplewiki shows that this adds 35% to the core time. The
                |decode competes with the encoders of the other tasks on the same executor.
                |Use it for a first run of a wiki, or after a change to the writer. See T437454."""
                    .stripMargin
        }
    }

    /** Job entrypoint
      *
      * @param args
      *   the parsed cli arguments
      */
    def main(args: Array[String]): Unit = {
        val params: Params = argsParser
            .parse(args, Params())
            .getOrElse(sys.exit(1))
        apply(params)
    }
}

/** Carries a Hadoop [[Configuration]] to the executors.
  *
  * `Configuration` is `Writable`, not `Serializable`. Spark has a wrapper for
  * this, but it is private to Spark. This wrapper keeps the settings of the
  * driver, which include the umask that makes the published files readable.
  *
  * The class is top level on purpose. A class inside an `object` can hold a
  * reference to that object, and the logger of the object is not serializable.
  */
class SerializableHadoopConfiguration(
    @transient private var conf: Configuration
) extends Serializable {

    def value: Configuration = conf

    private def writeObject(out: ObjectOutputStream): Unit = {
        out.defaultWriteObject()
        conf.write(out)
    }

    private def readObject(in: ObjectInputStream): Unit = {
        in.defaultReadObject()
        conf = new Configuration(false)
        conf.readFields(in)
    }
}
