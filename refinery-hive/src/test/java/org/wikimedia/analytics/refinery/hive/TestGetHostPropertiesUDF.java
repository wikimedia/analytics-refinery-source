// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.hive;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestGetHostPropertiesUDF {

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        GetHostPropertiesUDF getHostPropertiesUDF = new GetHostPropertiesUDF();
        getHostPropertiesUDF.initialize(initArguments);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetHostPropertiesUDF getHostPropertiesUDF = new GetHostPropertiesUDF();
        getHostPropertiesUDF.initialize(initArguments);
    }

    public static String join(List<String> l, String sep) {
        String res = "";
        for (int i = 0; i < l.size(); i++) {
            res += (i == 0) ? l.get(i) : sep + l.get(i);
        }
        return res;
    }

    @Test
    @FileParameters(
            value = "../refinery-core/src/test/resources/normalize_host_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testNormalizeHost(
            String test_description,
            String expectedProjectFamily,
            String expectedProject,
            String expectedQualifiers,
            String expectedTld,
            String uriHost
    ) throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetHostPropertiesUDF getHostPropertiesUDF = new GetHostPropertiesUDF();
        getHostPropertiesUDF.initialize(initArguments);

        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(uriHost) };
        Object[] res = (Object[]) getHostPropertiesUDF.evaluate(args);

        // Hacked for normalized_host.project_family change
        assertEquals("Result array has wrong length", 5, res.length);

        assertEquals(test_description + " - ProjectClass", expectedProjectFamily, res[0]);
        assertEquals(test_description + " - Project ", expectedProject, res[1]);
        assertEquals(test_description + " - Qualifiers", expectedQualifiers, join((List<String>)res[2], ";"));
        assertEquals(test_description + " - TLD", expectedTld, res[3]);
        assertEquals(test_description + " - ProjectFamily", expectedProjectFamily, res[4]);

        getHostPropertiesUDF.close();
    }

}