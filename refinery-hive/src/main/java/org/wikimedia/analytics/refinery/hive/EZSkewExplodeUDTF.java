package org.wikimedia.analytics.refinery.hive;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZHourlyData;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZStringParser;

/**
 * Takes a string encoded in the Pagecounts-EZ
 * format (every letter indicates the hour of the day followed by the
 * number of views):
 *
 * A7B234C34D324[...]
 *
 * And turns it into a maximum of 24 separate rows. Additionally, it
 * corrects the 1 hour skew problem reported in the legacy Pagecounts-EZ
 * dumps:
 *
 * https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pagecounts-ez#One_hour_skewing_issue
 *
 * Example of use:
 *     SELECT
 *       exploded.year,
 *       exploded.month,
 *       exploded.day,
 *       exploded.hour,
 *       exploded.views
 *     FROM {TABLE}
 *     LATERAL VIEW EZSkewExplodeUDTF(year, month, day, hourly);
 *
 * Result:
 * year month day hour views
 * 2020 2     1   1    30
 * 2020 2     1   2    50
 * 2020 2     1   3    32
 * 2020 2     1   4    60
 * 2020 2     1   5    38
 * [...]
 * 2020 2     2   0    45  <=== the unskewing of data makes all hours +1--see link above

 */

public class EZSkewExplodeUDTF extends GenericUDTF {
    private IntObjectInspector yearOI;
    private IntObjectInspector monthOI;
    private IntObjectInspector dayOI;
    private StringObjectInspector hourlyStringOI; // The encoded string of hourly values.

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)  throws UDFArgumentException {
        if (argOIs.length != 4)
            throw new UDFArgumentException("EZSkewExplode() takes exactly 4 arguments.");
        // input consists of 4 arguments
        yearOI = (IntObjectInspector) argOIs[0];
        monthOI = (IntObjectInspector) argOIs[1];
        dayOI = (IntObjectInspector) argOIs[2];
        hourlyStringOI = (StringObjectInspector) argOIs[3];

        // output inspectors -- an object with five fields!
        List<String> fieldNames = new ArrayList<String>(5);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(5);
        fieldNames.add("year");
        fieldNames.add("month");
        fieldNames.add("day");
        fieldNames.add("hour");
        fieldNames.add("views");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private ArrayList<Object[]> processInputRecord(int year, int month, int day, String hourly) {
        ArrayList<Object[]> result = new ArrayList<>();
        ArrayList<EZHourlyData> decodedCorrectedList = EZStringParser.ezStringToList(hourly, year, month, day);
        for(int i = 0; i < decodedCorrectedList.size(); i++) {
            EZHourlyData decodedHour = decodedCorrectedList.get(i);
            result.add(new Object[] {
                    decodedHour.year,
                    decodedHour.month,
                    decodedHour.day,
                    decodedHour.hour,
                    decodedHour.value
            });
        }
        return result;
    }

    @Override
    public void process(Object[] record) throws HiveException {
        int year = yearOI.get(record[0]);
        int month = monthOI.get(record[1]);
        int day = dayOI.get(record[2]);
        String hourly = hourlyStringOI.getPrimitiveJavaObject(record[3]);
        ArrayList<Object[]> results = processInputRecord(year, month, day, hourly);
        Iterator<Object[]> it = results.iterator();
        while (it.hasNext()) {
            Object[] r = it.next();
            forward(r);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}