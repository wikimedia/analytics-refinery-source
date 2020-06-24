package org.wikimedia.analytics.refinery.core.pagecountsEZ;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EZStringParser {
    public static ArrayList<EZHourlyData> ezStringToList (String ezString, int year, int month, int day) {
        Matcher m = Pattern.compile("[A-X]\\d+").matcher(ezString);
        ArrayList<EZHourlyData> result = new ArrayList<>();
        while (m.find()) {
            EZHourlyData decodedHour = decodeEZHour(m.group(), year, month, day);
            result.add(decodedHour);
        }
        return result;
    }

    public static EZHourlyData decodeEZHour (String encodedHour, int year, int month, int day) {
        char hourLetter = encodedHour.charAt(0);
        int hour = ((int) hourLetter - 'A');
        int views = Integer.parseInt(encodedHour.substring(1));
        Date uncorrectedDate = new Date(year, month - 1, day, hour, 0); // Dates are constructed with January as zero
        Calendar cal = Calendar.getInstance();
        cal.setTime(uncorrectedDate);
        cal.add(Calendar.HOUR, -1);
        int correctedYear = cal.get(Calendar.YEAR) - 1900; // For some insane reason java.util.Calendar's year zero is 1900
        int correctedMonth = cal.get(Calendar.MONTH ) + 1; // Calendar returns months with January as zero
        int correctedDay = cal.get(Calendar.DAY_OF_MONTH);
        int correctedHour = cal.get(Calendar.HOUR_OF_DAY);
        EZHourlyData decoded = new EZHourlyData(correctedYear, correctedMonth, correctedDay, correctedHour, views);
        return decoded;
    }
}
