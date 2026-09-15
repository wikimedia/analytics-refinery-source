package org.wikimedia.analytics.refinery.job.monitoring

import org.scalatest.{FlatSpec, Matchers}

/**
 * How an event log's name is turned back into the application id used to decide
 * whether it has been read before.
 *
 * Its own suite because it needs no SparkSession: these are the two pure
 * functions the dedup rests on, and getting them wrong means either reading an
 * application twice or never reading it at all.
 */
class TestEventLogNaming extends FlatSpec with Matchers {

    "applicationOf" should "leave a plain application id alone" in {
        SparkEventLogMetrics.applicationOf("hdfs:///var/log/spark/application_1757246400000_0001") should
            equal("application_1757246400000_0001")
    }

    it should "take the codec extension off a compressed log" in {
        SparkEventLogMetrics.applicationOf("hdfs:///var/log/spark/application_1757246400000_0001.lz4") should
            equal("application_1757246400000_0001")
    }

    it should "take the eventlog_v2_ prefix off a rolling log directory" in {
        SparkEventLogMetrics.applicationOf("hdfs:///var/log/spark/eventlog_v2_application_1757246400000_0001") should
            equal("application_1757246400000_0001")
    }

    "candidateApplicationIds" should "offer the name itself and the name without an attempt" in {
        // Which of the two is the application id cannot be told from the name: an
        // id ends in digits, and so does an attempt. Both are offered, and only
        // one of them can match an id that is actually in the table.
        SparkEventLogMetrics.candidateApplicationIds("application_1757246400000_0001_2") should equal(
            Seq("application_1757246400000_0001_2", "application_1757246400000_0001")
        )
    }

    it should "offer a log with no attempt under its own name" in {
        SparkEventLogMetrics.candidateApplicationIds("application_1757246400000_0001") should contain(
            "application_1757246400000_0001"
        )
    }

    /**
     * The reason this is equality rather than a prefix test. A YARN application
     * id is zero padded to four digits and then simply grows, so past ten
     * thousand applications an id becomes a prefix of a later one. Under the old
     * prefix match, `..._10000` was treated as already written whenever `..._1000`
     * was in the table, and was dropped on every run until its log aged out.
     */
    it should "not confuse an application with a longer-numbered one sharing its prefix" in {
        val later = SparkEventLogMetrics.candidateApplicationIds("application_1757246400000_10000")

        later should not contain "application_1757246400000_1000"
    }

    it should "not confuse a rolling log either" in {
        val later = SparkEventLogMetrics.candidateApplicationIds("eventlog_v2_application_1757246400000_10000")

        later should not contain "application_1757246400000_1000"
        later should contain("application_1757246400000_10000")
    }
}
