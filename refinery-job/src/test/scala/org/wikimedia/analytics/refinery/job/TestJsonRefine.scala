package org.wikimedia.analytics.refinery.job

import com.github.nscala_time.time.Imports.DateTime
import com.github.nscala_time.time.Imports._
import org.scalatest.{FlatSpec, Matchers}

class TestJsonRefine extends FlatSpec with Matchers {

    it should "hoursInBetween gives hours in between two DateTimes" in {
        val d1 = DateTime.now - 3.hours
        val d2 = DateTime.now

        val hoursShouldBe = Seq(
            (new DateTime(DateTime.now, DateTimeZone.UTC) - 3.hours).hourOfDay.roundFloorCopy,
            (new DateTime(DateTime.now, DateTimeZone.UTC) - 2.hours).hourOfDay.roundFloorCopy,
            (new DateTime(DateTime.now, DateTimeZone.UTC) - 1.hours).hourOfDay.roundFloorCopy
        )

        val hours = JsonRefine.hoursInBetween(d1, d2)
        hours should equal (hoursShouldBe)
    }
}