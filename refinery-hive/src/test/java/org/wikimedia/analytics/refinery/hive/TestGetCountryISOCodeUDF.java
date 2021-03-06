/*
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestGetCountryISOCodeUDF {

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        GetCountryISOCodeUDF getCountryISOCodeUDF = new GetCountryISOCodeUDF();

        getCountryISOCodeUDF.initialize(initArguments);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetCountryISOCodeUDF getCountryISOCodeUDF = new GetCountryISOCodeUDF();

        getCountryISOCodeUDF.initialize(initArguments);
    }

    @Test
    public void testEvaluateWithValidIPv4() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetCountryISOCodeUDF getCountryISOCodeUDF = new GetCountryISOCodeUDF();

        getCountryISOCodeUDF.initialize(initArguments);
        getCountryISOCodeUDF.configure(MapredContext.init(false, new JobConf()));

        //IPv4 addresses taken from MaxMind's test suite
        String ip = "81.2.69.160";
        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(ip) };
        Text result = (Text)getCountryISOCodeUDF.evaluate(args);
        assertEquals("ISO country code check", "GB", result.toString());
        getCountryISOCodeUDF.close();
    }

    @Test
    public void testEvaluateWithValidIPv6() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetCountryISOCodeUDF getCountryISOCodeUDF = new GetCountryISOCodeUDF();

        getCountryISOCodeUDF.initialize(initArguments);
        getCountryISOCodeUDF.configure(MapredContext.init(false, new JobConf()));

        //IPv6 representation of an IPv4 address taken from MaxMind's test suite
        String ip = "::ffff:81.2.69.160";
        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(ip) };
        Text result = (Text)getCountryISOCodeUDF.evaluate(args);
        assertEquals("ISO country code check", "GB", result.toString());
        getCountryISOCodeUDF.close();
    }

    @Test
    public void testEvaluateWithInvalidIP() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetCountryISOCodeUDF getCountryISOCodeUDF = new GetCountryISOCodeUDF();

        getCountryISOCodeUDF.initialize(initArguments);
        getCountryISOCodeUDF.configure(MapredContext.init(false, new JobConf()));

        //Invalid IPv4 addresses
        String ip = "-";
        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(ip) };
        Text result = (Text)getCountryISOCodeUDF.evaluate(args);
        assertEquals("ISO country code check", "--", result.toString());

        ip = null;
        args = new DeferredObject[] { new DeferredJavaObject(ip) };
        result = (Text)getCountryISOCodeUDF.evaluate(args);
        assertEquals("ISO country code check", "--", result.toString());
        getCountryISOCodeUDF.close();
    }

    @Test
    public void testIPWithoutIsoCode() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetCountryISOCodeUDF getCountryISOCodeUDF = new GetCountryISOCodeUDF();

        getCountryISOCodeUDF.initialize(initArguments);
        getCountryISOCodeUDF.configure(MapredContext.init(false, new JobConf()));

        String ip = "2a02:d500::"; // IP for EU
        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(ip) };
        Text result = (Text)getCountryISOCodeUDF.evaluate(args);
        assertEquals("ISO country code check", "--", result.toString());
        getCountryISOCodeUDF.close();
    }
}
