package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

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

}
