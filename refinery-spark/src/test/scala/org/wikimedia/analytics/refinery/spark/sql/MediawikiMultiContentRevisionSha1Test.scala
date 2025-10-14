package org.wikimedia.analytics.refinery.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class MediawikiMultiContentRevisionSha1Test extends FlatSpec with Matchers with DataFrameSuiteBase {

    "MediawikiMultiContentRevisionSha1Test" should "return null in case of null input" in {
        MediawikiMultiContentRevisionSha1.compute(null) should be(null)
    }

    "MediawikiMultiContentRevisionSha1Test" should "return null in case of empty input" in {
        MediawikiMultiContentRevisionSha1.compute(List.empty) should be(null)
    }

    "MediawikiMultiContentRevisionSha1Test" should "return first sha1 in case of single element input" in {
        val inputRow =  spark.sql("""select array(struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4"))""").collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.compute(inputRow) should equal("1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
    }

    "MediawikiMultiContentRevisionSha1Test" should "return computed sha1 in case of multi-element input" in {
        val inputRow =  spark.sql(
            """select array(
              |    struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4"),
              |    struct("mediainfo", "t15553qh44ewu6rrgv4xbrkpoutrvj3")
              |)""".stripMargin).collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.compute(inputRow) should equal("ie6h3mnqzrk3gi7yzvoqaqi5xeur9d3")
    }

    "MediawikiMultiContentRevisionSha1Test" should "return computed sha1 in case of multi-element input not sorted" in {
        val inputRow =  spark.sql(
            """select array(
              |    struct("mediainfo", "t15553qh44ewu6rrgv4xbrkpoutrvj3"),
              |    struct("main", "1jkqj7lxs8l999wu3jzlmzafwh6e2h4")
              |)""".stripMargin).collect.head.getAs[Seq[Row]](0)
        MediawikiMultiContentRevisionSha1.compute(inputRow) should equal("ie6h3mnqzrk3gi7yzvoqaqi5xeur9d3")
    }

}
