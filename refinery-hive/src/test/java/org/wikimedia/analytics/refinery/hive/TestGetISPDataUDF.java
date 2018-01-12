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

public class TestGetISPDataUDF {

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        GetISPDataUDF getISPDataUDF = new GetISPDataUDF();
        getISPDataUDF.initialize(initArguments);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetISPDataUDF getISPDataUDF = new GetISPDataUDF();
        getISPDataUDF.initialize(initArguments);
    }

    /*
     * Following tests data is the same as the one in refinery-core TestGeocodeISP
     */
    @Test
    public void testEvaluateWithValidIPv4() throws HiveException, IOException {
        //IPv4 addresses taken from MaxMind's test suite
        String ip = "82.99.17.96";
        Map<String, String> result = evaluate (ip);

        assertEquals("ISP check", "IP-Only Telecommunication Networks AB", result.get("isp"));
        assertEquals("Organization check", "Effectiv Solutions", result.get("organization"));
        assertEquals("Autonomous-system-organization check", "IP-Only", result.get("autonomous_system_organization"));
        assertEquals("Autonomous-system-number check", "12552", result.get("autonomous_system_number"));
    }

    @Test
    public void testEvaluateWithValidIPv6() throws HiveException, IOException {
        //IPv6 representation of an IPv4 address taken from MaxMind's test suite
        String ip = "::ffff:82.99.17.96";
        Map<String, String> result = evaluate (ip);

        assertEquals("ISP check", "IP-Only Telecommunication Networks AB", result.get("isp"));
        assertEquals("Organization check", "Effectiv Solutions", result.get("organization"));
        assertEquals("Autonomous-system-organization check", "IP-Only", result.get("autonomous_system_organization"));
        assertEquals("Autonomous-system-number check", "12552", result.get("autonomous_system_number"));
    }

    @Test
    public void testEvaluateWithInvalidIPs() throws HiveException, IOException {
        //Invalid IP
        String ip = "-";
        Map<String, String> result = evaluate(ip);

        assertEquals("ISP check", "Unknown", result.get("isp"));
        assertEquals("Organization check", "Unknown", result.get("organization"));
        assertEquals("Autonomous-system-organization check", "Unknown", result.get("autonomous_system_organization"));
        assertEquals("Autonomous-system-number check", "-1", result.get("autonomous_system_number"));

        ip = null;
        result = evaluate(ip);

        assertEquals("ISP check", "Unknown", result.get("isp"));
        assertEquals("Organization check", "Unknown", result.get("organization"));
        assertEquals("Autonomous-system-organization check", "Unknown", result.get("autonomous_system_organization"));
        assertEquals("Autonomous-system-number check", "-1", result.get("autonomous_system_number"));
    }

    private Map<String, String> evaluate(String ip) throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetISPDataUDF getISPDataUDF = new GetISPDataUDF();

        getISPDataUDF.initialize(initArguments);
        getISPDataUDF.configure(MapredContext.init(false, new JobConf()));

        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(ip) };
        Map<String, String> result = (Map<String, String>)getISPDataUDF.evaluate(args);
        getISPDataUDF.close();
        return result;
    }
}
