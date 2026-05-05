package org.wikimedia.analytics.refinery.job.mwhistory

import org.scalatest.{FlatSpec, Matchers}

class MWHistoryIncrementalWriterTest extends FlatSpec with Matchers {
  "MWHistoryIncrementalWriter" should "exist" in {
    MWHistoryIncrementalWriter.getClass.getSimpleName shouldEqual "MWHistoryIncrementalWriter$"
  }
}
