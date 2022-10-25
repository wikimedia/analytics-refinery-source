package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.{DataFrame, Row, RuntimeConfig, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

import javax.xml.stream.XMLStreamConstants
import scala.collection.mutable.ListBuffer
import scopt.OptionParser
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.stax2.XMLStreamReader2
import com.ctc.wstx.stax.WstxInputFactory

import java.io.BufferedReader
import java.io.StringReader
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{coalesce, col, count, explode, explode_outer, expr, lit, sum, udf}
import org.apache.spark.rdd.RDD
import org.wikimedia.analytics.refinery.spark.io.xml.BetweenTagsInputFormat
import org.wikimedia.analytics.refinery.tools.LogHelper

/**
 * Class providing a converter from an XML formatted HDFS FsImage
 * to an Hive table partition easier to query.
 *
 * The input is a large XML files stored on HDFS. To read it:
 *   - We are reading the file in parallel as an Hadoop file
 *   - We are using a custom Hadoop reader (BetweenTagsInputFormat) to parse the chunks between specific XML tags.
 *   - We parse the XML within the tags to fill the parameters of some case classes.
 *
 * After reading, the applied transformations are:
 *   - rebuild inodes absolute paths with a fixed point algorithm (mainly performing joins recursively)
 *   - Build a dataframe from the previous RDD
 *   - Aggregate the data by directory for an easier consumption through some large explodes
 *   - create or replace an Hive table partition
 *
 * Notice: The job parallelization level is computed from the main spark parameters:
 *   - maximum number of executors
 *   - number of executor cores
 * Changing those parameters will change the parallelization of the job. So you may also manually update the memory.
 */
object HdfsXMLFsImageConverter extends LogHelper {

    /**
     * HDFS block
     * Belongs to an inode.
     * @param id block uniq identifier
     * @param numBytes the size of the block in bytes
     */
    case class Block(
        id: Option[Long] = None,
        numBytes: Option[Long] = None
    )

    object Block {

        /**
         * Parse the XML representation of an HDFS block and create a <Block>
         *
         * Using iterative code instead of functional is an optimization to minimize object creation and GC.
         *
         * @param xmlStreamReader
         * @return a block instance
         */
        def parseXml(xmlStreamReader: XMLStreamReader2): Block = {
            var id, numBytes: Option[Long] = None
            while (xmlStreamReader.hasNext) {
                xmlStreamReader.next match {
                    case XMLStreamConstants.START_ELEMENT =>
                        val elName = xmlStreamReader.getName.toString
                        elName match {
                            case "id" => id = Some(xmlStreamReader.getElementText.toLong)
                            case "numBytes" => numBytes = Some(xmlStreamReader.getElementText.toLong)
                            case _ => {}:Unit  // continue
                        }
                    case _ => {}:Unit  // continue
                }
            }
            Block(id, numBytes)
        }

        /**
         * Spark schema of a block.
         * It's used to create a dataframe from an RDD.
         */
        val schema: StructType = StructType(Seq(
            StructField("id", LongType, nullable = false),
            StructField("num_bytes", LongType, nullable = false)
        ))
    }

    /**
     * HDFS inode
     * Could be a file, a directory, or a link.
     * @param id inode unique identifier. eg: 56809
     * @param typ type of the inode. eg: 'DIRECTORY', 'FILE'
     * @param name name of the inode. It doesn't contain the full path. eg: 'myfile.xml'
     * @param path path of the inode. It is reconstructed in `rebuildPath`. eg: '/path/to/myfile.xml'
     * @param replication how many times each inode blocks are replicated. eg: 3
     * @param mtime last modification time. eg: 1522925686575
     * @param atime last access time. eg: 1522925686576
     * @param preferredBlockSize preferred size for a block in bytes. eg: 134217728
     * @param permission HDFS inode permission. eg: 'hive:hdfs:rw-r--r--'
     * @param blocks the blocks containing the file data. Each one is a <Block>.
     * @param parentId the id of the parent inode. Only the root inode ('/') hasn't got a parent.
     *                 Used to link the directory structure.
     *                 Will be extracted by `rebuildPath` from the list of <Directory>.
     *                 eg: 16385
     * @param depth directory distance from root. It is reconstructed in `rebuildPath`. root is depth 1.
     *              eg: '/path/to/myfile.xml' => 4
     * @param totalBlocks number of blocks in the file or in all the files of the directory recursively.
     * @param totalUsefulBytes sum of blocks numBytes in the file or in all the files of the directory recursively.
     * @param totalReplicatedBytes same idea as totalUsefulBytes but increased by each block replication factor.
     * @param fileUnderConstruction Is the file under construction on HDFS.
     */
    case class Inode(
        id: Option[Long] = None,
        typ: Option[String] = None,
        name: Option[String] = None,
        path: Option[String] = None,
        replication: Option[Int] = None,
        mtime: Option[Long] = None,
        atime: Option[Long] = None,
        preferredBlockSize: Option[Long] = None,
        permission: Option[String] = None,
        blocks: Seq[Block] = Seq.empty,
        parentId: Option[Long] = None,
        depth: Option[Int] = None,
        totalBlocks: Option[Long] = None,
        totalUsefulBytes: Option[Long] = None,
        totalReplicatedBytes: Option[Long] = None,
        fileUnderConstruction: Boolean = false
    )

    object Inode {

        /**
         * Parse the XML representation of an HDFS inode and create an <Inode>
         *
         * Using iterative code instead of functional is an optimization to minimize object creation and GC.
         *
         * @param xmlStreamReader
         * @return an Inode instance
         */
        def parseXml(xmlStreamReader: XMLStreamReader2): Inode = {
            var id, mtime, atime, preferredBlockSize: Option[Long] = None
            var replication: Option[Int] = None
            var typ, name, path, permission: Option[String] = None
            val blocks: ListBuffer[Block] = ListBuffer[Block]()
            var fileUnderConstruction: Boolean = false
            while (xmlStreamReader.hasNext) {
                xmlStreamReader.next match {
                    case XMLStreamConstants.START_ELEMENT =>
                        val elName = xmlStreamReader.getName.toString
                        elName match {
                            case "id" => id = Some(xmlStreamReader.getElementText.toLong)
                            case "type" => typ = Some(xmlStreamReader.getElementText)
                            case "name" => name = Some(xmlStreamReader.getElementText)
                            case "path" => path = Some(xmlStreamReader.getElementText)
                            case "replication" => replication = Some(xmlStreamReader.getElementText.toInt)
                            case "mtime" => mtime = Some(xmlStreamReader.getElementText.toLong)
                            case "atime" => atime = Some(xmlStreamReader.getElementText.toLong)
                            case "preferredBlockSize" => preferredBlockSize = Some(xmlStreamReader.getElementText.toLong)
                            case "permission" => permission = Option(xmlStreamReader.getElementText)
                            case "block" => blocks += Block.parseXml(xmlStreamReader)
                            case "file-under-construction" => fileUnderConstruction = true
                            case _ => {}: Unit  // continue
                        }
                    case _ => {}:Unit  // continue
                }
            }
            val totalBlocks = Some(blocks.size.toLong)
            val blocksSize = blocks.map(_.numBytes.get).sum
            val totalUsefulBytes = Some(blocksSize)
            val totalReplicatedBytes = Some(replication.getOrElse(1) * blocksSize)
            Inode(
                id=id,
                typ=typ,
                name=name,
                path=path,
                replication=replication,
                mtime=mtime,
                atime=atime,
                preferredBlockSize=preferredBlockSize,
                permission=permission,
                blocks=blocks,
                fileUnderConstruction=fileUnderConstruction,
                totalBlocks=totalBlocks,
                totalUsefulBytes=totalUsefulBytes,
                totalReplicatedBytes=totalReplicatedBytes
            )
        }

        /**
         * Spark schema of an inode.
         * It's used to create a dataframe from an RDD.
         */
        val schema: StructType = StructType(Seq(
            StructField("id", LongType, nullable = false),
            StructField("parent_id", LongType, nullable = true),
            StructField("typ", StringType, nullable = true),
            StructField("name", StringType, nullable = true),
            StructField("path", StringType, nullable = false),
            StructField("path_depth", IntegerType, nullable = false),
            StructField("replication", IntegerType, nullable = true),
            StructField("mtime", LongType, nullable = true),
            StructField("atime", LongType, nullable = true),
            StructField("preferred_block_size", LongType, nullable = true),
            StructField("permission", StringType, nullable = true),
            StructField("blocks", ArrayType(Block.schema, containsNull = false), nullable = false)
        ))
    }

    /**
     * Description of an element of the HDFS directory structure
     *
     * In the HDFS FSImage, the relationships between inodes are represented by a list of directories.
     * For each directory on HDFS, a list of ids describes the inodes it contains.
     *
     * @param id id of an inode (an HDFS directory). eg: 12345
     * @param children ids of the children inodes (the children are some HDFS files or directories).
     *                 eg: [67890, 09876]
     */
    case class Directory(
        id: Option[Long] = None,
        children: Seq[Long] = Seq.empty
    )

    object Directory {

        /**
         * Parse the XML representation of an element of the HDFS tree and create a <Directory>
         *
         * Using iterative code instead of functional is an optimization to minimize object creation and GC.
         *
         * @param xmlStreamReader
         * @return a Directory instance
         */
        def parseXml(xmlStreamReader: XMLStreamReader2): Directory = {
            var id: Option[Long] = None
            val children: ListBuffer[Long] = ListBuffer[Long]()
            while (xmlStreamReader.hasNext) {
                xmlStreamReader.next match {
                    case XMLStreamConstants.START_ELEMENT =>
                        val elName = xmlStreamReader.getName.toString
                        elName match {
                            case "parent" => id = Some(xmlStreamReader.getElementText.toLong)
                            case "child" => children += xmlStreamReader.getElementText.toLong
                            case _ => {}: Unit // continue
                        }
                    case _ => {}: Unit // continue
                }
            }
            Directory(id, children)
        }
    }

    /**
     * Spark UDF used to explode the inode paths
     * Generates 1 element per directory level.
     * Does not generate a line for root.
     * eg: "/a/b/c.parquet" => ["/a", "/a/b", "/a/b/c.parquet"]
     */
    val pathAndParentsUDF: UserDefinedFunction = udf((path: String) => {
        val pathParts = path.split("/")
        pathParts.foldLeft(Array.empty[String])((acc, folder) => {
            if (acc.isEmpty) Array(folder)
            else acc :+ s"${acc.last}/$folder"
        })
    })

    // This `spark` object variable is used by other lazy object vals.
    // It's a variable, not a val, because the Spark session is instanced by the apply function.
    var spark: Option[SparkSession] = None
    def setSparkSession(_spark: SparkSession): Unit = spark = Some(_spark)

    lazy val sparkConf: RuntimeConfig = {spark.get.conf}

    // Number of cores per executor. Set from outside of this job.
    lazy val cores: Int = { sparkConf.get("spark.executor.cores").toInt }

    // Max number of executors. Set from outside of this job.
    lazy val executors: Int = { sparkConf.get("spark.dynamicAllocation.maxExecutors").toInt }

    // Compute RDD parallelization to the total number of cores
    lazy val rddParallelization: Int = { cores * executors }

    // Size of an HDFS block.
    lazy val blockSize: Int = { sparkConf.get("spark.sql.files.maxPartitionBytes").toInt }

    /**
     * Creates an analytics dataset from the XML fsimage.
     *
     * @param params parsed Params including the input path to the XML file, and an output path.
     * @return Unit
     */
    def apply(params: Params): Unit = {

        val spark: SparkSession = SparkSession.builder.getOrCreate()
        setSparkSession(spark)

        val checkpointsDir = s"${params.checkpointsDir}/${spark.sparkContext.applicationId}"
        spark.sparkContext.setCheckpointDir(checkpointsDir)

        val inodes: RDD[Inode] = readInodes(params, spark)
            .cache

        log.info(s"Inodes found in XML source: ${inodes.count}")

        val directories: RDD[Directory] = readDirectories(params, spark)
            .cache

        log.info(s"Directories found in XML source: ${directories.count}")

        val cachedBaseForJoin = prepareToRebuildPath(inodes, directories)
            .cache

        log.info(s"Preparation for ${cachedBaseForJoin.count} inode paths reconstruction.")

        List(inodes, directories).foreach(_.unpersist()) // Removing RDD from cache after use in the last `count`.

        log.info(s"rddParallelization = $rddParallelization .")

        val cachedInodesWithPath = rebuildPath(params, cachedBaseForJoin)

        /**
         * Optimization:
         * The following explode of HDFS paths by parents makes the dataset grows larger, with an expected factor of 4.
         * Thus the need to split the job into more tasks than executors in order to evenly distribute the work.
         */
        val dfShuffleParallelization = 4 * rddParallelization  // Should produce 4 tasks per compute entity.
        log.info(s"Setting spark.sql.shuffle.partitions to $dfShuffleParallelization .")
        sparkConf.set("spark.sql.shuffle.partitions", dfShuffleParallelization)

        val inodesDf: DataFrame = buildDataframe(spark, cachedInodesWithPath)
            .cache

        log.info(s"inodesDF generated (${inodesDf.count}).")

        cachedInodesWithPath.unpersist()  // Removing RDD from cache after use (in the last `count`).

        aggregateAndPrepareResult(params, inodesDf)
            .coalesce(params.maxOutputFiles)
            .write
            .mode(SaveMode.Overwrite)
            .insertInto(params.outputTable)

        log.info(s"Inodes with aggregated sizes saved as parquet in ${params.outputTable}/${params.snapshot}.")
    }

    /**
     * Creates an RDD of Directories using the XML between-tags reader
     *
     * This dataset represents all edges in the filesystem tree. Each "directory" here is a parent inodeId, with its
     * associated list of children inode Ids.
     */
    private def readDirectories(params: Params, spark: SparkSession): RDD[Directory] = {
        spark.sparkContext.hadoopConfiguration.set("betweentagsinputformat.record.starttag", "<directory>")
        spark.sparkContext.hadoopConfiguration.set("betweentagsinputformat.record.endtag", "</directory>")

        /**
         * Optimization:
         * All the directories XML elements are collocated in few blocks in the source file.
         * It is better to set a smaller split size so that the interesting portions are split among workers.
         * The splits containing no directories takes almost no time to parse.
         */
        val directoriesSize = getfsImageXMLFileSize(params.fsImageXMLPath) * params.proportionOfDirectoriesInFsImageXML
        val splitSize = roundToDivideOfBlock((directoriesSize/(cores*executors)).toLong, blockSize)
        optimizeSplitSize(spark, splitSize)

        spark.sparkContext
            .newAPIHadoopFile(
                params.fsImageXMLPath,
                classOf[BetweenTagsInputFormat],
                classOf[LongWritable],
                classOf[Text],
                new Configuration(spark.sparkContext.hadoopConfiguration)
            )
            .map(_._2.toString)
            // Optimization: Use mapPartition in order to create only 1 factory per partition.
            .mapPartitions(it => {
                val xmlInputFactory = new WstxInputFactory()
                it.map(s => {
                    val buffreader = new BufferedReader(new StringReader(s))
                    val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
                    Directory.parseXml(xmlReader)
                })
            })
    }

    /**
     * Set the split max size parameter in the Hadoop configuration.
     * Spark distribute the reading of a file on HDFS by attributing to each task a small split of the file to read.
     * It's used to determine the number of tasks generated to read the whole file.
     * @param spark the current spark session
     * @param splitSize the chosen split size
     */
    private def optimizeSplitSize(spark: SparkSession, splitSize: Long): Unit = {
        log.info(s"Setting the split size to $splitSize")
        spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.maxsize", splitSize.toString)
    }

    /**
     * Creates an RDD of Inodes using the XML between-tags reader
     *
     * This dataset includes all HDFS elements (inodes) of the file system including files and directories.
     *   - There is no link to the parent (directory or root). This data is built later.
     *   - Only the basename of the filesystem element is included here. The abspath is computed later.
     *
     * A small number of inodes are some files under construction. We currently filter them out.
     */
    private def readInodes(params: Params, spark: SparkSession): RDD[Inode] = {
        spark.sparkContext.hadoopConfiguration.set("betweentagsinputformat.record.starttag", "<inode>")
        spark.sparkContext.hadoopConfiguration.set("betweentagsinputformat.record.endtag", "</inode>")

        /**
         * Optimization:
         * By default, When reading a file Spark is generating as many tasks as the number of blocks.
         * As we have many executors, we try to generate enough tasks for all of them.
         * 67MB is a quarter of an HDFS block.
         */
        val splitSize = roundToDivideOfBlock(getfsImageXMLFileSize(params.fsImageXMLPath)/(cores*executors), blockSize)
        optimizeSplitSize(spark, splitSize)

        spark.sparkContext
            .newAPIHadoopFile(
                params.fsImageXMLPath,
                classOf[BetweenTagsInputFormat],
                classOf[LongWritable],
                classOf[Text],
                new Configuration(spark.sparkContext.hadoopConfiguration)
            )
            .map(_._2.toString)
            // Optimization: Use mapPartition in order to create only 1 factory per partition.
            .mapPartitions(it => {
                val xmlInputFactory = new WstxInputFactory()
                it.flatMap(s => {
                    val buffreader = new BufferedReader(new StringReader(s))
                    val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
                    val inode = Inode.parseXml(xmlReader)
                    // Drop files under construction:
                    //   - in the FileUnderConstructionSection element, those inodes have no types.
                    //   - in the INodeSection element, those inodes have an element file-under-construction.
                    if (inode.typ.isEmpty || inode.fileUnderConstruction) Seq() else Seq(inode)
                })
            })
    }

    /**
     * Prepare an RDD containing inodes with their parents.
     *
     * @param inodes RDD of inodes
     * @param directories RDD of directories
     * @return baseForJoin, a dataset of Tuple(3) eg: (name||path, 1, inode)
     *         Where the second element of the tuple will be the depth of the node after the rebuilding paths step.
     */
    def prepareToRebuildPath(
        inodes: RDD[Inode],
        directories:RDD[Directory]
    ): RDD[(String, Int, Inode)]  = {

        val child2parent: RDD[(Option[Long], Option[Long])] = directories.flatMap(d => {
            d.children.map(c => (Some(c), d.id))
        })

        val inodesWithParents = inodes
            .keyBy(_.id)
            .leftOuterJoin(child2parent)
            .map(
                t => t._2._1.copy(  // `t._2._1` is the <Inode> from inodesWithParents
                    parentId = t._2._2.flatten  // `flatten` simplifies `Option[Option[...]]` to `Option[]`
                )
            )

       inodesWithParents.map(i => (i.name.getOrElse(i.path.get), 1, i))
    }

    /**
     * Rebuild the inode paths from the structure of directories
     *
     * - Prepare a dataset to easily access the parent of an inode.
     * - Put the inodes into a their own structure
     * - fix-point algorithm to build the full path of every node.
     *     In each loop of this algorithm a join is performed to find the next parent directory of each node, and
     *     rebuild its path. It stops when there is no more inode to linked with the root directory.
     *
     * 2 variables contain dataset with progressively refined data:
     *     - rootedInodes lists all inodes with a path to the root
     *     - inodesToJoin lists all the other inodes
     */
    def rebuildPath(
        params: Params,
        cachedBaseForJoin: RDD[(String, Int, Inode)]
    ): RDD[(Option[Long], (String, Int, Inode))] = {

        // To prepare for the join, rootedInodes is keyed by the inode's id. Whereas inodesToJoin is keyed by the parent
        // inode's id.
        var rootedInodes = cachedBaseForJoin.filter(_._3.parentId.isEmpty).keyBy(_._3.id).cache()
        var inodesToJoin = cachedBaseForJoin.filter(_._3.parentId.isDefined).keyBy(_._3.parentId).cache()
        var loops = 0
        List(rootedInodes, inodesToJoin).foreach(_.checkpoint())
        var stillToJoin = inodesToJoin.count
        var joined = rootedInodes.count
        cachedBaseForJoin.unpersist()  // Removing RDD from cache after use in the last `count`s.
        val listOfRDDsToUnPersist = ListBuffer[RDD[_]](rootedInodes, inodesToJoin)
        while (stillToJoin != 0 && loops < params.maxDirectoryRecursionDepth) {
            loops += 1
            log.info(s"starting loop $loops with $stillToJoin inodes still to join and $joined joined inodes...")

            // inodesToJoin and rootedInodes are joined using the first element of their respective tuples.
            val joinedInodes: RDD[(Option[Long], ((String, Int, Inode), Option[(String, Int, Inode)]))] = inodesToJoin
                // Explicitly setting the number of partitions to be used for the join.
                .leftOuterJoin(rootedInodes, numPartitions = rddParallelization)
                .cache

            // Later, when we access `_._2`, we access to the result of the previous join, as a tuple:
            //   - `_._2._1` is the left element of the join (an element of inodesToJoin)
            //   - `_._2._2` is the right element of the join (an element of rootedInode)
            // If the rootedInode is defined, this node has bin linked to the root, it's *rooted*!
            rootedInodes = rootedInodes
                .union(
                    joinedInodes
                        .filter(_._2._2.isDefined)
                        .map(t => {
                            val inodeInfo = t._2._1
                            val parent = t._2._2.get  // (path, directory depth, inode)
                            // struct = (id, (path||name, current depth, inode))
                            //   This struct is keyed by id, ready for the next iteration join.
                            (inodeInfo._3.id, (parent._1 + "/" + inodeInfo._1, parent._2 + inodeInfo._2, inodeInfo._3))
                        })
                )

            // The result is a struct: (id, (path||name, current depth, inode))
            inodesToJoin = joinedInodes
                .filter(_._2._2.isEmpty)
                .map(t => (t._1, t._2._1))
                .cache

            List(rootedInodes, inodesToJoin).foreach({ rdd =>
                rdd.cache
                if (loops % params.checkpointsEveryXLoops == 0) rdd.checkpoint
            })
            stillToJoin = inodesToJoin.count
            joined = rootedInodes.count
            listOfRDDsToUnPersist.foreach(_.unpersist()) // Removing RDD from cache after use in the last `count`s.
            listOfRDDsToUnPersist.clear()
            listOfRDDsToUnPersist += (joinedInodes, rootedInodes, inodesToJoin)
        }

        // Managing checkpoints and unpersisting cached data.
        // (This is technical code, not functional code.)
        if (loops % params.checkpointsEveryXLoops != 0) {
            rootedInodes.checkpoint()
            val count = rootedInodes.count // Fill cache & checkpointing
            log.info(s"Inodes paths reconstructed (${count}).")
            // Keep only rootedInodes in cache. It's used later.
            listOfRDDsToUnPersist.foreach(rdd => if (rdd != rootedInodes) rdd.unpersist())
            listOfRDDsToUnPersist.clear()
        }

        rootedInodes
    }

    /**
     * Transform the list of inodes to a Spark DataFrame
     *
     * The DataFrame schema is as defined in Inode.schema
     */
    private def buildDataframe(spark: SparkSession, cachedInodesWithPath: RDD[(Option[Long], (String, Int, Inode))]): DataFrame = {

        val rows = cachedInodesWithPath.map(t => {
            val path = t._2._1
            val depth = t._2._2
            val inode = t._2._3
            val blocks = inode.blocks.map(b => Row.fromTuple((b.id.get, b.numBytes.get))).toArray
            Row.fromTuple((
                inode.id.get,
                inode.parentId.getOrElse(null),
                inode.typ.orNull,
                inode.name.orNull,
                path,
                depth,
                inode.replication.getOrElse(null),
                inode.mtime.getOrElse(null),
                inode.atime.getOrElse(null),
                inode.preferredBlockSize.getOrElse(null),
                inode.permission.getOrElse(null),
                blocks
            ))
        })

        spark.createDataFrame(rows, Inode.schema)
    }

    /**
     * Perform aggregations on the dataset
     *   - get how many files there are in a directory recursively
     *   - get the disk space occupied by a directory
     *   - ...
     */
    private def aggregateAndPrepareResult(params: Params, inodesDf: DataFrame): DataFrame = {

        val df = inodesDf
            .filter("typ IS NOT NULL")

        val aggregatedSizes = df
            .select(
                col("path"),
                coalesce(col("replication"), lit(1L)).as("replication"),
                coalesce(col("preferred_block_size"), lit(0L)).as("preferred_block_size"),
                explode_outer(col("blocks")).as("block")
            )
            .select(
                col("path"),
                col("replication"),
                col("preferred_block_size"),
                coalesce(col("block.num_bytes"), lit(0L)).as("num_bytes")
            )
            .groupBy(
                col("path"),
                col("replication"),
                col("preferred_block_size")
            )
            .agg(
                count(lit(1L)).as("blocks_count"),
                sum("num_bytes").as("blocks_size")
            )
            .select(
                col("path"),
                col("blocks_count"),
                col("blocks_size"),
                (col("replication") * col("blocks_size")).as("replicated_blocks_size"),
                (col("blocks_count") * col("preferred_block_size")).as("reserved_size"),
                (col("replication") * col("blocks_count") * col("preferred_block_size"))
                    .as("replicated_reserved_size")
            )
            .select(
                // A lot of data gets generated through path-explosion with this explode.
                explode(pathAndParentsUDF(col("path"))).as("path"),
                col("blocks_count"),
                col("blocks_size"),
                col("replicated_blocks_size"),
                col("reserved_size"),
                col("replicated_reserved_size")
            )
            .groupBy(
                col("path")
            )
            .agg(
                sum("blocks_count").as("blocks_count"),
                sum("blocks_size").as("blocks_size"),
                sum("replicated_blocks_size").as("replicated_blocks_size"),
                sum("reserved_size").as("reserved_size"),
                sum("replicated_reserved_size").as("replicated_reserved_size"),
            )

        val aggregatedCounts = df
            .filter("typ == 'FILE'")
            // A lot of data gets generated through path-explosion with this explode.
            .select(explode(pathAndParentsUDF(col("path"))).as("path"))
            .groupBy(col("path").as("aggregated_counts_path"))
            .agg(count(lit(1L)).cast(LongType).as("files_count"))

        df
            .join(
                aggregatedSizes.withColumnRenamed("path", "aggregated_sizes_path"),
                col("path") === col("aggregated_sizes_path"),
                "left_outer")
            .join(
                aggregatedCounts,
                col("path") === col("aggregated_counts_path") &&
                    col("typ") === lit("DIRECTORY"),
                "left_outer")
            .withColumn(
                "average_file_size",
                expr("CASE WHEN files_count IS NOT NULL THEN blocks_size / files_count ELSE NULL END")
                    .cast(LongType))
            .drop("aggregated_counts_path", "aggregated_sizes_path", "blocks")
            .withColumnRenamed("typ", "type")
            .withColumn("split_permission", expr("split(permission, ':')"))
            .drop("permission")
            .withColumn("user", expr("split_permission[0]"))
            .withColumn("group", expr("split_permission[1]"))
            .withColumn("permission", expr("split_permission[2]"))
            .drop("split_permission")
            .withColumn("snapshot", lit(params.snapshot))
    }

    /**
     * Removes the temporary directory used to store the Spark checkpoints
     * Those checkpoints could occupy a lot of place on HDFS (eg: 1TB generated during some tests.)
     * @param params
     */
    private def removeCheckpointDir(params: Params): Unit = {
        val spark = SparkSession.builder.getOrCreate()
        val path: Path = new Path(s"${params.checkpointsDir}/${spark.sparkContext.applicationId}")
        val fs: FileSystem = path.getFileSystem(new Configuration)
        log.info(s"Removing checkpoint dir: ${path}")
        fs.delete(path, true)  // Second parameter stands for recursive.
    }

    var fsImageXMLSize: Option[Long] = None  // Simple memoization
    /**
     * Get the HDFS file size of the source fsimage as XML
     * @param pathString
     * @return a number of bytes
     */
    private def getfsImageXMLFileSize(pathString: String): Long = {
        if (fsImageXMLSize.isEmpty) {
            val path: Path = new Path(pathString)
            val fs: FileSystem = path.getFileSystem(new Configuration)
            val size = fs.getContentSummary(path).getLength
            fsImageXMLSize = Some(size)
        }
        fsImageXMLSize.get
    }

    /**
     * Take the next smallest divide of the HDFS block size by a power of 2.
     * @param length a byte length to round
     * @param HDFSBlockSize
     * @return a division by a power of 2 of the HDFS block size
     */
    private def roundToDivideOfBlock(length: Long, HDFSBlockSize: Long): Long = {
        var result = HDFSBlockSize
        while (result >= length) { result = result / 2 }
        result
    }

    /**
     * Config class for CLI argument parser using scopt
     */
    case class Params(
        fsImageXMLPath: String = "", // Parameter required
        outputTable: String = "", // Parameter required
        snapshot: String = "", // Parameter required
        // Maximum number of joins to perform between inodes and parent when rebuilding the paths.
        // Most directories have less than 40 parent directories recursively.
        maxDirectoryRecursionDepth: Integer = 40,
        // Maximum numbers of parquet files produced.
        // Used in DF.coalesce before writing.
        // Currently the dataset is ~10GB. Producing 20 files means each weights ~500MB.
        // Warning: lowering this value needs more workers memory to retrieve the partition before storing it.
        maxOutputFiles: Integer = 20,
        // We use checkpointing when rebuilding the inodes paths recursively in order:
        //   * to clean the RDD dependencies tree
        //   * to avoid the whole re-computation in case of errors
        // We perform a checkpoint every x loop.
        checkpointsEveryXLoops: Integer = 5,
        checkpointsDir: String = "hdfs:///tmp/HdfsXMLFsImageConverter", // HDFS url
        // To optimize parallelization, we need to know roughly the proportion of blocks containing directory XML
        // elements in the FsImage XML file.
        proportionOfDirectoriesInFsImageXML: Double = 0.10  // Only 10% of the blocks contains directories data.
    )

    /**
     * Define the command line options parser
     */
    val argsParser: OptionParser[Params] = new OptionParser[Params]("HDFS XML FsImage converter") {
        head("HDFS XML FsImage converter", "")
        note("This job extracts data from an HDFS FsImage (XML formated).")
        help("help") text ("Prints this usage text")

        opt[String]('i', "fsimage_xml_path") required() valueName ("<fsimage_xml_Path>") action { (x, p) =>
            p.copy(fsImageXMLPath = x)
        } text ("The HDFS file path to the HDFS FsImage formatted as XML.")

        opt[String]('o', "output_table") required() valueName ("<output_table>") action { (x, p) =>
            p.copy(outputTable = x)
        } text ("The name of the Hive table to store the result.")

        opt[String]('s', "snapshot") required() valueName ("<snapshot>") action { (x, p) =>
            p.copy(snapshot = x)
        } text ("The name of the Hive partition to replace.")

        opt[Int]('f', "max_output_files") optional() valueName ("<max_output_files>") action { (x, p) =>
            p.copy(maxOutputFiles = x)
        } text ("Maximum numbers of parquet files produced.")

        opt[Int]('m', "max_directory_recursion_depth") optional() valueName ("<max_directory_recursion_depth>") action { (x, p) =>
            p.copy(maxDirectoryRecursionDepth = x)
        } text ("Maximum number of joins to perform between inodes and parent when rebuilding the paths.")

        opt[Int]('c', "checkpoints_every_x_loop") optional() valueName ("<checkpoints_every_x_loop>") action { (x, p) =>
            p.copy(checkpointsEveryXLoops = x)
        } text ("Perform a checkpoint every x loops of the algorithm.")

        opt[String]('d', "checkpoints_dir") optional() valueName ("<checkpoints_dir>") action { (x, p) =>
            p.copy(checkpointsDir = x)
        } text ("Checkpoint directory (The appId will be added at the end)")

        opt[Double]('p', "proportion_of_directories_in_fsimage_xml") optional() valueName ("<proportion_of_directories_in_fsimage_xml>") action { (x, p) =>
            p.copy(proportionOfDirectoriesInFsImageXML = x)
        } text ("Proportion of blocks containing directory XML elements in the FsImage XML file.")
    }

    /**
     * Job entrypoint
     * @param args the parsed cli arguments
     */
    def main(args: Array[String]): Unit = {
        val params: Params = argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        // Exit non-zero if failed.
        try {
            apply(params)
        } finally {
            removeCheckpointDir(params)
        }
    }
}