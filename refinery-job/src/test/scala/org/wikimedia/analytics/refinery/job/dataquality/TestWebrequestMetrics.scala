package org.wikimedia.analytics.refinery.job.dataquality

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.Assert.{assertEquals, assertFalse}
import org.scalatest.{FlatSpec, Matchers}


class TestWebrequestMetrics extends FlatSpec with Matchers with DataFrameSuiteBase {
    val duplicateKey =  QueryStringAnalyzer.splitAndTrim("key1=value1;key2=value2;key1=value3")
    val duplicateKeyValue = QueryStringAnalyzer.splitAndTrim("key1=value1;key2=value2;key1=value1")
    val multiPartValue = QueryStringAnalyzer.splitAndTrim("key1=value1;key2=multi;part;key3=value3")


    it should "count duplicate keys in the x_analytics field" in {
        val distinctValues = false
        assertEquals(1,  CountDuplicateKeys(duplicateKey, distinctValues))
        assertEquals(Seq(KeyValue("key1", "value1"), KeyValue("key2", "value2"), KeyValue("key1", "value3")),
            duplicateKey)
    }

    it should "count duplicate key/value pairs in the x_analytics field" in {
        val distinctValues = true
        assertEquals(0, CountDuplicateKeys(duplicateKey, distinctValues))
        assertEquals(1, CountDuplicateKeys(duplicateKeyValue, distinctValues))
        assertEquals(Seq(KeyValue("key1", "value1"), KeyValue("key2", "value2"), KeyValue("key1", "value1")),
            duplicateKeyValue)
    }

    it should "handle multipart key/value pairs" in {
        val distinctValues = false
        assertEquals(0, CountDuplicateKeys(multiPartValue, distinctValues))
        assertEquals(3, multiPartValue.length)
        assertEquals(Seq(KeyValue("key1", "value1"), KeyValue("key2", "multi;part"), KeyValue("key3", "value3")),
            multiPartValue)
    }

    it should "check if a key/value contains has multipart values" in {
        assertTrue(HasMultipartValue(multiPartValue))
        assertFalse(HasMultipartValue(duplicateKey))
    }

    it should "generate stats for a query string" in {
        assertEquals(Tuple3(1, 0, 0), QueryStringAnalyzer("key1=value1;key2=multi;part;key3=value3"))
    }
}
