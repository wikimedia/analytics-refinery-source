package org.wikimedia.analytics.refinery.spark.deequ

/**
 * Helper trait and class to implement write operations to HDFS and Iceberg
 * in Deequ conversion utilities.
 */

protected trait Writer {
    def output(target: String): Writer

    def output: String

    def save(): Unit
}

/**
 * Data quality jobs output targets. Currently we allow writing to iceberg
 * and text (on HDFS).
 * @param iceberg
 * @param text
 */
protected case class DataQualitytWriter(iceberg: IcebergWriter, text: TextWriter)

protected object DataQualitytWriter {
    def apply(iceberg: IcebergWriter): DataQualitytWriter = {
        DataQualitytWriter(iceberg, DefaultTextWriter)
    }
}

/**
 * A dummy writer for classes that don't need to write to text.
 */
protected object DefaultTextWriter extends TextWriter {
    override def save(): Unit = None

    override def output(path: String): TextWriter = this

    override def output: String = ""
}

