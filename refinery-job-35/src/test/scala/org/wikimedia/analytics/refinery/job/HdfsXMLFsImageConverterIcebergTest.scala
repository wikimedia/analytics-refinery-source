package org.wikimedia.analytics.refinery.job

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Runs HdfsXMLFsImageConverter on a small XML fsimage and writes to a local Iceberg table.
 */
class HdfsXMLFsImageConverterIcebergTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val tmpDir: String = Files.createTempDirectory("hdfs-usage-iceberg").toAbsolutePath.toString
  val table = "local.wmf_data_ops.hdfs_usage_v2"

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("HdfsXMLFsImageConverterIcebergTest")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.session.timeZone", "UTC")
    // The job reads these to size its splits and partitions.
    .config("spark.executor.cores", "1")
    .config("spark.dynamicAllocation.maxExecutors", "2")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.sql.extensions",              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local",           "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type",      "hadoop")
    .config("spark.sql.catalog.local.warehouse", s"$tmpDir/warehouse")
    .getOrCreate()

  // /, /user, /user/alice, /user/alice/a.parquet (2 blocks, replication 3), /user/alice/b.txt (1 block, replication 2).
  val fsImageXml: String =
    """<?xml version="1.0"?>
      |<fsimage>
      |<INodeSection><lastInodeId>16389</lastInodeId><numInodes>5</numInodes>
      |<inode><id>16385</id><type>DIRECTORY</type><name></name><mtime>1</mtime><permission>hdfs:hadoop:0755</permission><nsquota>-1</nsquota><dsquota>-1</dsquota></inode>
      |<inode><id>16386</id><type>DIRECTORY</type><name>user</name><mtime>1</mtime><permission>hdfs:hadoop:0755</permission><nsquota>-1</nsquota><dsquota>-1</dsquota></inode>
      |<inode><id>16387</id><type>DIRECTORY</type><name>alice</name><mtime>1</mtime><permission>alice:alice:0700</permission><nsquota>-1</nsquota><dsquota>-1</dsquota></inode>
      |<inode><id>16388</id><type>FILE</type><name>a.parquet</name><replication>3</replication><mtime>1</mtime><atime>1</atime><preferredBlockSize>268435456</preferredBlockSize><permission>alice:alice:0644</permission><blocks><block><id>1</id><genstamp>1</genstamp><numBytes>100</numBytes></block><block><id>2</id><genstamp>1</genstamp><numBytes>50</numBytes></block></blocks><storagePolicyId>0</storagePolicyId></inode>
      |<inode><id>16389</id><type>FILE</type><name>b.txt</name><replication>2</replication><mtime>1</mtime><atime>1</atime><preferredBlockSize>268435456</preferredBlockSize><permission>alice:alice:0644</permission><blocks><block><id>3</id><genstamp>1</genstamp><numBytes>10</numBytes></block></blocks><storagePolicyId>0</storagePolicyId></inode>
      |</INodeSection>
      |<INodeDirectorySection>
      |<directory><parent>16385</parent><child>16386</child></directory>
      |<directory><parent>16386</parent><child>16387</child></directory>
      |<directory><parent>16387</parent><child>16388</child><child>16389</child></directory>
      |</INodeDirectorySection>
      |</fsimage>
      |""".stripMargin

  val xmlPath: String = {
    val path = s"$tmpDir/fsimage.xml"
    Files.write(java.nio.file.Paths.get(path), fsImageXml.getBytes(StandardCharsets.UTF_8))
    s"file://$path"
  }

  override def beforeAll(): Unit = {
    spark.sql(
      s"""CREATE TABLE $table (
         |  id bigint, parent_id bigint, type string, name string, path string, path_depth int,
         |  replication int, mtime bigint, atime bigint, preferred_block_size bigint,
         |  blocks_count bigint, blocks_size bigint, replicated_blocks_size bigint,
         |  reserved_size bigint, replicated_reserved_size bigint, files_count bigint,
         |  average_file_size bigint, `user` string, `group` string, permission string, day date
         |) USING iceberg
         |PARTITIONED BY (day)
         |TBLPROPERTIES ('format-version' = '2')""".stripMargin)
    spark.sql(s"ALTER TABLE $table WRITE ORDERED BY type, path_depth, path")
  }

  override def afterAll(): Unit = spark.stop()

  def run(day: String): Unit = HdfsXMLFsImageConverter.apply(HdfsXMLFsImageConverter.Params(
    fsImageXMLPath = xmlPath,
    outputTable = table,
    day = day,
    checkpointsDir = s"file://$tmpDir/checkpoints"
  ))

  "HdfsXMLFsImageConverter" should "aggregate sizes and file counts for each directory" in {
    run("2026-10-19")

    val alice = spark.sql(s"SELECT * FROM $table WHERE day = DATE '2026-10-19' AND path = '/user/alice'").collect()
    alice.length shouldBe 1
    val row = alice.head
    row.getAs[String]("type") shouldBe "DIRECTORY"
    row.getAs[Int]("path_depth") shouldBe 3
    row.getAs[Long]("files_count") shouldBe 2L
    row.getAs[Long]("blocks_size") shouldBe 160L
    row.getAs[Long]("replicated_blocks_size") shouldBe 470L  // 3 * 150 + 2 * 10
    row.getAs[Long]("average_file_size") shouldBe 80L
    row.getAs[String]("user") shouldBe "alice"
    row.getAs[String]("group") shouldBe "alice"
    row.getAs[String]("permission") shouldBe "0700"
    row.getAs[Date]("day") shouldBe Date.valueOf("2026-10-19")

    val file = spark.sql(s"SELECT * FROM $table WHERE path = '/user/alice/a.parquet'").collect().head
    file.getAs[String]("type") shouldBe "FILE"
    file.getAs[Int]("path_depth") shouldBe 4
    file.getAs[Long]("blocks_count") shouldBe 2L
    file.getAs[Long]("blocks_size") shouldBe 150L
  }

  it should "replace the rows of the same day on a rerun" in {
    run("2026-10-19")

    spark.sql(s"SELECT count(*) FROM $table WHERE day = DATE '2026-10-19'").collect().head.getLong(0) shouldBe 5L
  }

  it should "keep the rows of other days" in {
    run("2026-10-26")

    val counts = spark.sql(s"SELECT day, count(*) FROM $table GROUP BY day ORDER BY day").collect()
      .map(r => (r.getDate(0).toString, r.getLong(1)))
    counts shouldBe Array(("2026-10-19", 5L), ("2026-10-26", 5L))
  }

  "HdfsXMLFsImageConverter.argsParser" should "reject a day that is not YYYY-MM-DD" in {
    val args = Array("--fsimage_xml_path", "x", "--output_table", "t", "--day", "2026-10")
    HdfsXMLFsImageConverter.argsParser.parse(args, HdfsXMLFsImageConverter.Params()) shouldBe None
  }
}
