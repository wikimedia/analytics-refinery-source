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

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestGetPageviewInfoUDF {

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        GetPageviewInfoUDF udf = new GetPageviewInfoUDF();
        udf.initialize(initArguments);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2, value3};
        GetPageviewInfoUDF udf = new GetPageviewInfoUDF();
        udf.initialize(initArguments);
    }


    @Test
    @FileParameters(
        value = "../refinery-core/src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testGetPageviewInfo(
        String test_description,
        String project,
        String dialect,
        String pageTitle,
        boolean is_pageview,
        boolean is_legacy_pageview,
        boolean is_app_pageview,
        String ip_address,
        String x_forwarded_for,
        String uri_host,
        String uri_path,
        String uri_query,
        String http_status,
        String content_type,
        String user_agent
    ) throws HiveException, IOException {
        if (is_pageview) {
            ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector value3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2, value3};
            GetPageviewInfoUDF udf = new GetPageviewInfoUDF();

            udf.initialize(initArguments);

            GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[] {
                    new GenericUDF.DeferredJavaObject(uri_host),
                    new GenericUDF.DeferredJavaObject(uri_path),
                    new GenericUDF.DeferredJavaObject(uri_query)
            };
            Map<String, String> result = (Map<String, String>)udf.evaluate(args);
            udf.close();

            assertEquals("Project check -" + test_description, project, result.get(GetPageviewInfoUDF.PROJECT_KEY));
            assertEquals("Dialect check -" + test_description, dialect, result.get(GetPageviewInfoUDF.DIALECT_KEY));
            assertEquals("Page_Title check - " + test_description, pageTitle, result.get(GetPageviewInfoUDF.PAGE_TITLE_KEY));
        }
    }

    @Test
    public void testGetPageviewInfoNull() throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2, value3};
        GetPageviewInfoUDF udf = new GetPageviewInfoUDF();

        udf.initialize(initArguments);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[] {
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(null)
        };
        Map<String, String> result = (Map<String, String>)udf.evaluate(args);
        udf.close();

        assertEquals("Empty project", PageviewDefinition.UNKNOWN_PROJECT_VALUE, result.get(GetPageviewInfoUDF.PROJECT_KEY));
        assertEquals("Empty dialect", PageviewDefinition.UNKNOWN_DIALECT_VALUE, result.get(GetPageviewInfoUDF.DIALECT_KEY));
        assertEquals("Empty page_title", PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE, result.get(GetPageviewInfoUDF.PAGE_TITLE_KEY));
    }

}