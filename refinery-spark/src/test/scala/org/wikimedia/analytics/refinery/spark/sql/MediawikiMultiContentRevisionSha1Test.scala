package org.wikimedia.analytics.refinery.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class MediawikiMultiContentRevisionSha1Test extends FlatSpec with Matchers with DataFrameSuiteBase {

    "computeForRows" should "return null in case of null input" in {
        MediawikiMultiContentRevisionSha1.computeForRows(null) should be(null)
    }

    "computeForRows" should "return null in case of empty input" in {
        MediawikiMultiContentRevisionSha1.computeForRows(List.empty) should be(null)
    }

    "computeForRows" should "return first sha1 in case of single element input" in {
        val inputRow =  spark.sql("""select array(struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4"))""").collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.computeForRows(inputRow) should equal("1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
    }

    "computeForRows" should "return null if a NULL-struct is passed in the array" in {
        val inputRow =  spark.sql("""select array(NULL)""").collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.computeForRows(inputRow) should be(null)
    }

    "computeForRows" should "return null if a partial NULL-struct is passed in the array" in {
        val inputRow =  spark.sql("""select array(struct("main", NULL))""").collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.computeForRows(inputRow) should be(null)
    }

    "computeForRows" should "return first sha1 in case of one element input and null" in {
        val inputRow =  spark.sql("""select array(struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4"), NULL)""").collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.computeForRows(inputRow) should equal("1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
    }

    "computeForRows" should "return computed sha1 in case of multi-element input" in {
        val inputRow =  spark.sql(
            """select array(
              |    struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4"),
              |    struct("mediainfo", "t15553qh44ewu6rrgv4xbrkpoutrvj3")
              |)""".stripMargin).collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.computeForRows(inputRow) should equal("ie6h3mnqzrk3gi7yzvoqaqi5xeur9d3")
    }

    "computeForRows" should "return computed sha1 in case of multi-element input not sorted" in {
        val inputRow =  spark.sql(
            """select array(
              |    struct("mediainfo", "t15553qh44ewu6rrgv4xbrkpoutrvj3"),
              |    struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
              |)""".stripMargin).collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.computeForRows(inputRow) should equal("ie6h3mnqzrk3gi7yzvoqaqi5xeur9d3")
    }

    "computeForTuples" should "return computed sha1 in case of multi-element input not sorted" in {
        val inputTuples = Seq(
            ("mediainfo", "t15553qh44ewu6rrgv4xbrkpoutrvj3"),
            ("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
        )
        MediawikiMultiContentRevisionSha1.computeForTuples(inputTuples) should equal("ie6h3mnqzrk3gi7yzvoqaqi5xeur9d3")
    }

    "computeForTuples" should "ignore a NULL-struct when passed in the array" in {
        val inputTuples =  Seq(
            null,
            ("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
        )
        MediawikiMultiContentRevisionSha1.computeForTuples(inputTuples) should be("1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
    }

    "computeForTuples" should "ignore a partial NULL-struct when passed in the array" in {
        val inputTuples = Seq(
            ("mediainfo", "t15553qh44ewu6rrgv4xbrkpoutrvj3"),
            ("main", null)
        )
        MediawikiMultiContentRevisionSha1.computeForTuples(inputTuples) should be("t15553qh44ewu6rrgv4xbrkpoutrvj3")
    }

}
