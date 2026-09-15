package org.wikimedia.analytics.refinery.spark.utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

/** Carries a Hadoop [[Configuration]] to the executors.
  *
  * `Configuration` is `Writable`, not `Serializable`. This wrapper keeps the
  * settings of the driver, so each task can open the same file system.
  *
  * The class is top level on purpose. A class inside an `object` can hold a
  * reference to that object.
  *
  * Spark 3.5 twin of the class declared in refinery-spark's
  * `HdfsFileFingerprintWriter.scala`. refinery-spark is compiled against Spark
  * 3.1, so this module cannot depend on it; keep the two in step if either
  * changes.
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
