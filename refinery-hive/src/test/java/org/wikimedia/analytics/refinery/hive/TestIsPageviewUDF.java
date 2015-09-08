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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(JUnitParamsRunner.class)
public class TestIsPageviewUDF {
    IsPageviewUDF udf = null;
    ObjectInspector[] initArguments = null;


    @Before
    public void setUp() throws HiveException{
        udf = new IsPageviewUDF();

        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        initArguments = new ObjectInspector[]{valueOI, valueOI, valueOI, valueOI, valueOI, valueOI, valueOI};
        udf.initialize(initArguments);
    }

    @Test
    @FileParameters(
        value = "../refinery-core/src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    // this mapper cannot deal with reading strings formed like
    // the x analytics map: "{"WMF-Last-Access":"12-Aug-2015","https":"1"}
    public void testIsPageview(
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
    ){

        GenericUDF.DeferredJavaObject uri_host_udf = new GenericUDF.DeferredJavaObject(uri_host);
        GenericUDF.DeferredJavaObject uri_path_udf = new GenericUDF.DeferredJavaObject(uri_path);
        GenericUDF.DeferredJavaObject uri_query_udf = new GenericUDF.DeferredJavaObject(uri_query);
        GenericUDF.DeferredJavaObject http_status_udf = new GenericUDF.DeferredJavaObject(http_status);
        GenericUDF.DeferredJavaObject content_type_udf = new GenericUDF.DeferredJavaObject(content_type);
        GenericUDF.DeferredJavaObject user_agent_udf = new GenericUDF.DeferredJavaObject(user_agent);
        GenericUDF.DeferredJavaObject x_analytics_udf = new GenericUDF.DeferredJavaObject("");

        GenericUDF.DeferredObject[] args = {uri_host_udf, uri_path_udf, uri_query_udf,
            http_status_udf, content_type_udf, user_agent_udf, x_analytics_udf};


        try {
            assertEquals(test_description, is_pageview, udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testIsPageviewXAnalyticsPreview() throws HiveException{

        GenericUDF.DeferredJavaObject uri_host = new GenericUDF.DeferredJavaObject("en.wikipedia");
        GenericUDF.DeferredJavaObject uri_path = new GenericUDF.DeferredJavaObject("/wiki/Horseshoe%20crab#anchor");
        GenericUDF.DeferredJavaObject uri_query = new GenericUDF.DeferredJavaObject("-");
        GenericUDF.DeferredJavaObject http_status = new GenericUDF.DeferredJavaObject("200");
        GenericUDF.DeferredJavaObject content_type = new GenericUDF.DeferredJavaObject("text/html");
        GenericUDF.DeferredJavaObject user_agent = new GenericUDF.DeferredJavaObject("turnip");
        GenericUDF.DeferredJavaObject x_analytics = new GenericUDF.DeferredJavaObject("{'blah':1,'preview':1}");

        GenericUDF.DeferredObject[] args = {uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics};

        boolean isPageview = (boolean) udf.evaluate(args);

        assertFalse("Preview requests should not be consider pageviews", isPageview);
    }



    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArgumentsTooFew() throws HiveException{

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArgumentsFew = new ObjectInspector[]{value1, value2};
        udf.initialize(initArgumentsFew);
    }


    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArgumentsTooMany() throws HiveException{

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArgumentsTooMany = new ObjectInspector[]{value1, value2, value1, value2, value1, value2, value1, value2};
        udf.initialize(initArgumentsTooMany);
    }

    // UDF should work with variable arguments
    public void testMinMaxNumberOfArguments() throws HiveException{

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value4 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value5 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value6 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2, value3, value4, value5, value6};
        udf.initialize(initArguments);

        ObjectInspector value7 = ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector
        );

        ObjectInspector[] initArgumentsMax = new ObjectInspector[]{value1, value2, value3, value4, value5, value6, value7};
        udf.initialize(initArgumentsMax);


    }

}