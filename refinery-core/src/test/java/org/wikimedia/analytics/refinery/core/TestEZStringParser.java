package org.wikimedia.analytics.refinery.core;

import org.junit.Test;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZHourlyData;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZStringParser;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class TestEZStringParser {
    @Test
    public void testNumberOfReturnedValues() {
        final ArrayList<EZHourlyData> rows = EZStringParser.ezStringToList("A3B9C2D6E3F5G5H4J4K47L18N1O4P3Q6R3S3T3U1V4W1X1", 2020, 5, 10);
        assertEquals(rows.size(), 22);
    }
    @Test
    public void testSkewIsCorrect() {
        final ArrayList<EZHourlyData> rows = EZStringParser.ezStringToList("A3B9C2D6E3F5G5H4J4K47L18N1O4P3Q6R3S3T3U1V4W1X1", 2020, 5, 10);
        int targetHour = 1;
        int correctedViews = 2;
        int resultViews;
        for(EZHourlyData row : rows){
            int hour = row.hour;
            if (hour == targetHour) {
                resultViews = row.value;
                assertEquals(correctedViews, resultViews);
            }
        }
    }
    @Test
    public void testMoveToNextMonth() {
        final ArrayList<EZHourlyData> rows = EZStringParser.ezStringToList("A3B9C2D6E3F5G5H4J4K47L18N1O4P3Q6R3S3T3U1V4W1X1", 2020, 5, 1);
        int targetHour = 23;
        int expectedMonth = 4;
        int expectedDay = 30;
        for(EZHourlyData row : rows){
            int hour = row.hour;
            if (hour == targetHour) {
                int day = row.day;
                int month = row.month;
                assertEquals(expectedDay, day);
                assertEquals(expectedMonth, month);
            }
        }
    }
    @Test public void testLongString() {
        String longString = "A2227B1623C1319D1057E921F894G936H1124I1833J2932K4201L5519M4880N4699O5524P6263Q6190R6746S7082T5554U5173V5113W4537X3238";
        final ArrayList<EZHourlyData> rows = EZStringParser.ezStringToList(longString, 2020, 5, 10);
        assertEquals(rows.size(), 24);
    }
}
