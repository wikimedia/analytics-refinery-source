package org.wikimedia.analytics.refinery.hive;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;


/**
 * We test the most popular browser + device combos (from sampled logs)
 * and the ua parser reporting on on those.
 * <p/>
 * Test failing will indicate than the newer version of ua parser
 * is significantly different from the prior one.
 */
@RunWith(JUnitParamsRunner.class)
public class TestUAParserUDFUserAgentMostPopular {
    ObjectInspector[] initArguments = null;
    GetUAPropertiesUDF getUAPropertiesUDF = null;
    JSONParser parser = null;

    @Before
    public void setUp() throws HiveException {
        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        initArguments = new ObjectInspector[]{valueOI};
        getUAPropertiesUDF = new GetUAPropertiesUDF();
        getUAPropertiesUDF.initialize(initArguments);
        parser = new JSONParser();

    }

    @After
    public void tearDown() throws HiveException {

        try {
            getUAPropertiesUDF.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }




    @Test
    @FileParameters(
            value = "../refinery-core/src/test/resources/ua_most_popular_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testMatchingOfMostPopularUA(String uaString, String jsonMapResult) throws HiveException, ParseException {

        DeferredJavaObject ua = new DeferredJavaObject(uaString);

        // decode expected output and turn it into an object
        Object obj = parser.parse(jsonMapResult);
        JSONObject expected_ua = (JSONObject) obj;

        DeferredObject[] args1 = {ua};
        HashMap<String, String> computed_ua = (HashMap<String, String>) getUAPropertiesUDF
                .evaluate(args1);

        assertEquals("OS name check", expected_ua.get("os_family"),
                computed_ua.get("os_family"));

        assertEquals("OS major version check", expected_ua.get("os_major"),
                computed_ua.get("os_major"));

        assertEquals("OS minor version check", expected_ua.get("os_minor"),
                computed_ua.get("os_minor"));

        assertEquals("browser check", expected_ua.get("browser_family"),
                computed_ua.get("browser_family"));

        assertEquals("browser major version check", expected_ua.get("browser_major"),
                computed_ua.get("browser_major"));

        assertEquals("device check", expected_ua.get("device_family"),
                computed_ua.get("device_family"));

    }

}



