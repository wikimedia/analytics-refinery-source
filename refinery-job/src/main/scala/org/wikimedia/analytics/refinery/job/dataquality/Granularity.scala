package org.wikimedia.analytics.refinery.job.dataquality

/**
  * This object determines the seasonality given a granularity value.
  *
  * The  seasonality-cycle should indicate the strongest seasonality
  * period of the metric group. The seasonality cycle is given in data points.
  * For example: if the data has hourly granularity and daily seasonality
  * (timeseries pattern repeats every day) the seasonality-cycle value should
  * be 24, because it takes 24 data points to complete a seasonality cycle.
  *
  * Usage:
  * 'name' is a predefined accessor of an Enumeration 'name' value
  * val g = Granularity.withName("hourly")
  *
  * def f(g: Granularity.Value) = g match {
  * case Granularity.hourly => println("hourly")
  * }
  *
  * For an explanation of enums in scala see: https://www.baeldung.com/scala/enumerations
  */

import scala.language.implicitConversions

object Granularity extends Enumeration {

    // defining just a data container
    // the Enumeration class provides an inner class named Val,
    // which can be extended to add additional attributes:
    protected case class GranularityValue(name:String, seasonalityCycle: Int, defaultCycles: Int) extends super.Val(name) {
        def maxTimeSeriesLength: Int = seasonalityCycle * defaultCycles
    }
    // converts from enumeration Value class (with only name accessor)
    // to our extension which adds three accessors:seasonalityCycle,defaultCycles and maxTimeSeriesLength
    implicit def valueToGranularityVal(x: Value): GranularityValue = x.asInstanceOf[GranularityValue]

    // seasonality is 24 hours this is hourly data, 30 cycles is 720 datapoints
    val hourly = GranularityValue("hourly", 24, 30)
    // daily data, 30 cycles is 210 points
    val daily = GranularityValue("daily", 7, 30)
    // yearly data, 2 cycles is 24 points
    val monthly = GranularityValue("monthly", 12, 2)

}
