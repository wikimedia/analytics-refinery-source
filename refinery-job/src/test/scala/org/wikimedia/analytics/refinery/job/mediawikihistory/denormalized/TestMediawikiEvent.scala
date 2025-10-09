package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.Row

import org.scalatest.{FlatSpec, Matchers}

class TestMediawikiEvent extends FlatSpec with Matchers {

  "MediawikiEventRevisionDetails" should "correctly extract revisionDeletedParts from flag" in {

    MediawikiEventRevisionDetails.getRevDeletedParts(1) should equal(Seq("text"))
    MediawikiEventRevisionDetails.getRevDeletedParts(2) should equal(Seq("comment"))
    MediawikiEventRevisionDetails.getRevDeletedParts(3) should equal(Seq("text", "comment"))
    MediawikiEventRevisionDetails.getRevDeletedParts(4) should equal(Seq("user"))
    MediawikiEventRevisionDetails.getRevDeletedParts(5) should equal(Seq("text", "user"))
    MediawikiEventRevisionDetails.getRevDeletedParts(6) should equal(Seq("comment", "user"))
    MediawikiEventRevisionDetails.getRevDeletedParts(7) should equal(Seq("text", "comment", "user"))
  }

  it should "correctly extract revisionDeletedPartsAreSuppressed from flag" in {
    (0 until 8).foreach(i => {
      MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed(i) should equal(false)
    })

    (8 until 16).foreach(i => {
      MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed(i) should equal(true)
    })

  }

  "MediawikiEvent" should "be unmarshable from a Spark Row that contains null Seqs" in {
    val seqOfNulls = Seq.fill(77)(null)
    val row = Row.fromSeq(seqOfNulls)

    val event = MediawikiEvent.fromRow(row)
    event should not be null
    event.userDetails.userBlocksHistorical should(equal(None))
  }
}
