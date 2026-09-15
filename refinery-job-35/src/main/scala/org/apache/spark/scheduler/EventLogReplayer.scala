package org.apache.spark.scheduler

import java.io.InputStream

/**
 * Gives refinery code access to Spark's own event log replay.
 *
 * `ReplayListenerBus` is `private[spark]`, so this class lives in Spark's
 * package to reach it -- the same approach the repo already takes in
 * `org.apache.spark.sql.hive.thriftserver` and
 * `org.apache.spark.sql.execution.datasources.custom.jdbc`.
 *
 * The bus itself holds no state across a replay; the listener does. Feeding
 * several files of a rolling event log through one instance therefore
 * reconstructs the application exactly as a single file log would.
 *
 * @param listener Receives the deserialised events, exactly as the live bus
 *                 would have delivered them.
 */
class EventLogReplayer(listener: SparkListenerInterface) {

    private val bus = new ReplayListenerBus()
    bus.addListener(listener)

    /**
     * Replays one event log file.
     *
     * Truncation is tolerated: an application killed before Spark finished
     * writing still describes the work it completed, which is most of the value
     * of reading logs rather than reporting from inside the driver.
     *
     * @param in         The log file's contents, already decompressed.
     * @param sourceName Name used in Spark's own log messages about the replay.
     */
    def replay(in: InputStream, sourceName: String): Unit =
        bus.replay(in, sourceName, maybeTruncated = true)
}
