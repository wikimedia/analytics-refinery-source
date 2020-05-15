package org.wikimedia.analytics.refinery.core.pagecountsEZ;

public class EZHourlyData {
    public int year;
    public int month;
    public int day;
    public int hour;
    public int value;

    public EZHourlyData(int year, int month, int day, int hour, int value) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.value = value;
    }
}
