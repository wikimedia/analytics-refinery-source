/**
 * Copyright (C) 2014  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.hive;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Test;

public class TestEZSkewExplodeUDTF {
    private static List<Object> evaluate(final GenericUDTF udtf, int year, int month, int day, String hourly)
            throws HiveException {
        final List<Object> out = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                out.add(input);
            }
        });
        udtf.process(new Object[] { year, month, day, hourly });
        return out;
    }

    @Test
    public void testNumberOfReturnedValues() throws HiveException {
        final EZSkewExplodeUDTF udtf = new EZSkewExplodeUDTF();
        udtf.initialize(getOIs());
        final List<Object> rows = evaluate(udtf, 2020, 5, 10, "A3B9C2D6E3F5G5H4J4K47L18N1O4P3Q6R3S3T3U1V4W1X1");
        assertEquals(rows.size(), 22);
    }
    @Test
    public void testSkewIsCorrect() throws HiveException {
        final EZSkewExplodeUDTF udtf = new EZSkewExplodeUDTF();
        udtf.initialize(getOIs());
        final List<Object> rows = evaluate(udtf, 2020, 5, 10, "A3B9C2D6E3F5G5H4J4K47L18N1O4P3Q6R3S3T3U1V4W1X1");
        int targetHour = 1;
        int correctedViews = 2;
        int resultViews;
        for(Object row : rows){
            Object[] rowArray = (Object[]) row;
            int hour = (int)rowArray[3];
            if (hour == targetHour) {
                resultViews = (int)rowArray[4];
                assertEquals(correctedViews, resultViews);
            }
        }
    }
    @Test
    public void testMoveToNextMonth() throws HiveException {
        final EZSkewExplodeUDTF udtf = new EZSkewExplodeUDTF();
        udtf.initialize(getOIs());
        final List<Object> rows = evaluate(udtf, 2020, 5, 1, "A3B9C2D6E3F5G5H4J4K47L18N1O4P3Q6R3S3T3U1V4W1X1");
        int targetHour = 23;
        int expectedMonth = 4;
        int expectedDay = 30;
        for(Object row : rows){
            Object[] rowArray = (Object[]) row;
            int hour = (int)rowArray[3];
            if (hour == targetHour) {
                int day = (int) rowArray[2];
                int month = (int) rowArray[1];
                assertEquals(expectedDay, day);
                assertEquals(expectedMonth, month);
            }
        }
    }
    @Test public void testLongString() throws HiveException {
        final EZSkewExplodeUDTF udtf = new EZSkewExplodeUDTF();
        udtf.initialize(getOIs());
        String longString = "A2227B1623C1319D1057E921F894G936H1124I1833J2932K4201L5519M4880N4699O5524P6263Q6190R6746S7082T5554U5173V5113W4537X3238";
        final List<Object> rows = evaluate(udtf, 2020, 5, 10, longString);
        assertEquals(rows.size(), 24);
    }
    private ObjectInspector[] getOIs() {
        return new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        };
    }
}