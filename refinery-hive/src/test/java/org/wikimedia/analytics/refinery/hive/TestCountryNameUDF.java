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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestCountryNameUDF {

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        CountryNameUDF countryNameUDF = new CountryNameUDF();

        countryNameUDF.initialize(initArguments);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        CountryNameUDF countryNameUDF = new CountryNameUDF();

        countryNameUDF.initialize(initArguments);
    }

    @Test
    public void testEvaluateWithValidCountryCode() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        CountryNameUDF countryNameUDF = new CountryNameUDF();

        countryNameUDF.initialize(initArguments);
        countryNameUDF.configure(MapredContext.init(false, new JobConf()));

        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject("IE") };
        Text result = (Text)countryNameUDF.evaluate(args);
        assertEquals("ISO valid country code check", "Ireland", result.toString());
        countryNameUDF.close();
    }

    @Test
    public void testEvaluateWithInvalidCountryCode() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        CountryNameUDF countryNameUDF = new CountryNameUDF();

        countryNameUDF.initialize(initArguments);
        countryNameUDF.configure(MapredContext.init(false, new JobConf()));

        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject("--") };
        Text result = (Text)countryNameUDF.evaluate(args);
        assertEquals("ISO invalid country code check", "Unknown", result.toString());
    }

}
