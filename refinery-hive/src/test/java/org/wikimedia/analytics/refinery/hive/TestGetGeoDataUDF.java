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

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestGetGeoDataUDF {

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        GetGeoDataUDF getGeoDataUDF = new GetGeoDataUDF();
        getGeoDataUDF.initialize(initArguments);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetGeoDataUDF getGeoDataUDF = new GetGeoDataUDF();
        getGeoDataUDF.initialize(initArguments);
    }

    @Test
    public void testEvaluateWithValidIPv4() throws HiveException, IOException {
        //IPv4 addresses taken from MaxMind's test suite
        String ip = "81.2.69.160";
        Map<String, String> result = evaluate (ip);

        assertEquals("Continent check", "Europe", result.get("continent"));
        assertEquals("ISO country code check", "GB", result.get("country_code"));
        assertEquals("Country check", "United Kingdom", result.get("country"));
        assertEquals("Subdivision check", "England", result.get("subdivision"));
        assertEquals("City check", "London", result.get("city"));
        assertEquals("Postal code check", null, result.get("postal_code"));
        assertEquals("Latitude check", null, result.get("latitude"));
        assertEquals("Longitude check", null, result.get("longitude"));
        assertEquals("Timezone check", "Europe/London", result.get("timezone"));
    }

    @Test
    public void testEvaluateWithValidIPv6() throws HiveException, IOException {
        //IPv6 representation of an IPv4 address taken from MaxMind's test suite
        String ip = "::ffff:81.2.69.160";
        Map<String, String> result = evaluate (ip);

        assertEquals("Continent check", "Europe", result.get("continent"));
        assertEquals("ISO country code check", "GB", result.get("country_code"));
        assertEquals("Country check", "United Kingdom", result.get("country"));
        assertEquals("Subdivision check", "England", result.get("subdivision"));
        assertEquals("City check", "London", result.get("city"));
        assertEquals("Postal code check", null, result.get("postal_code"));
        assertEquals("Latitude check", null, result.get("latitude"));
        assertEquals("Longitude check", null, result.get("longitude"));
        assertEquals("Timezone check", "Europe/London", result.get("timezone"));
    }

    @Test
    public void testEvaluateWithInvalidIP() throws HiveException, IOException {
        //Invalid IP
        String ip = "-";
        Map<String, String> result = evaluate(ip);

        assertEquals("Continent check", "Unknown", result.get("continent"));
        assertEquals("ISO country code check", "--", result.get("country_code"));
        assertEquals("Country check", "Unknown", result.get("country"));
        assertEquals("Subdivision check", "Unknown", result.get("subdivision"));
        assertEquals("City check", "Unknown", result.get("city"));
        assertEquals("Postal code check", null, result.get("postal_code"));
        assertEquals("Latitude check", null, result.get("latitude"));
        assertEquals("Longitude check", null, result.get("longitude"));
        assertEquals("Timezone check", "Unknown", result.get("timezone"));

        ip = null;
        result = evaluate(ip);

        assertEquals("Continent check", "Unknown", result.get("continent"));
        assertEquals("ISO country code check", "--", result.get("country_code"));
        assertEquals("Country check", "Unknown", result.get("country"));
        assertEquals("Subdivision check", "Unknown", result.get("subdivision"));
        assertEquals("City check", "Unknown", result.get("city"));
        assertEquals("Postal code check", null, result.get("postal_code"));
        assertEquals("Latitude check", null, result.get("latitude"));
        assertEquals("Longitude check", null, result.get("longitude"));
        assertEquals("Timezone check", "Unknown", result.get("timezone"));
    }

    private Map<String, String> evaluate(String ip) throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetGeoDataUDF getGeoDataUDF = new GetGeoDataUDF();

        getGeoDataUDF.initialize(initArguments);
        getGeoDataUDF.configure(MapredContext.init(false, new JobConf()));

        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(ip) };
        Map<String, String> result = (Map<String, String>)getGeoDataUDF.evaluate(args);
        getGeoDataUDF.close();
        return result;
    }
}
