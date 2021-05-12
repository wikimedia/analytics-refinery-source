package org.wikimedia.analytics.refinery.hive;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class TestUAParserUDFUserAgentRecognition {
    ObjectInspector[] initArguments = null;
    GetUAPropertiesUDF getUAPropertiesUDF = null;

    @Before
    public void setUp() throws HiveException {
        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        initArguments = new ObjectInspector[]{valueOI};
        getUAPropertiesUDF = new GetUAPropertiesUDF();
        getUAPropertiesUDF.initialize(initArguments);

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
    public void testHappyCase() throws HiveException {

        DeferredJavaObject ua1 = new DeferredJavaObject(
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:19.0) Gecko/20100101 Firefox/19.0");
        DeferredJavaObject ua2 = new DeferredJavaObject(
                "Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405");
        DeferredJavaObject ua3 = new DeferredJavaObject(
                "Mozilla/5.0 (iPad; CPU OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53");


        DeferredObject[] args1 = { ua1 };
        HashMap<String, String> evaled = (HashMap<String, String>) getUAPropertiesUDF
                .evaluate(args1);
        assertEquals("OS name check", (new String("Ubuntu")),
                evaled.get("os_family").toString());
        assertEquals("Browser name check", (new String("Firefox")),
                evaled.get("browser_family").toString());


        DeferredObject[] args2 = { ua2 };
        evaled = (HashMap<String, String>) getUAPropertiesUDF
                .evaluate(args2);
        assertEquals("OS name check", (new String("iOS")),
                evaled.get("os_family").toString());

        assertEquals("Browser name check", (new String("Mobile Safari UI/WKWebView")),
                evaled.get("browser_family").toString());


        DeferredObject[] args3 = { ua3 };
        evaled = (HashMap<String, String>) getUAPropertiesUDF
                .evaluate(args3);
        assertEquals("OS name check", (new String("iOS")),
                evaled.get("os_family").toString());
        assertEquals("Browser name check", (new String("Mobile Safari")),
                evaled.get("browser_family").toString());

    }

    /**
     * Tests what we return when browser is empty.
     *
     * UA parser will return this as "browser obj" for an empty user agent string:
     * {
     * user_agent: {family: "Other", major: null, minor: null, patch: null},
     * os: {family: "Other", major: null, minor: null, patch: null, patch_minor: null},
     * device: {family: "Other"}
     * }
     * UDFs returns something like the following:
     * {
     * "device_family":"Other",
     * "browser_major":"-",
     * "os_family":"Other",
     * "os_major":"-",
     * "browser_family":"Other",
     * "os_minor":"-"
     * }
     **/
    @Test
    public void testEmptyUA() throws HiveException {
        DeferredJavaObject ua1 = new DeferredJavaObject("");

        DeferredObject[] args = { ua1 };

        HashMap<String, String> evaled = (HashMap<String, String>) getUAPropertiesUDF
                .evaluate(args);

        String resultOSName = evaled.get("os_family");
        String resultBrowserName = evaled.get("browser_family");
        String resultOsMinor = evaled.get("os_minor");
        assertEquals("OS name check", (new String("Other")),
                resultOSName.toString());
        assertEquals("Browser name check", (new String("Other")),
                resultBrowserName.toString());

        assertEquals("OS minor", (new String("-")),
                resultOsMinor.toString());
    }

    /**
     * Tests what we return when browser is null
     * Is this right? Maybe we should return just null
     *
     * UA parser will return this as "browser obj" for an empty user agent string:
     * {
     * user_agent: {family: "Other", major: null, minor: null, patch: null},
     * os: {family: "Other", major: null, minor: null, patch: null, patch_minor: null},
     * device: {family: "Other"}
     * }
     * UDFs returns something like the following:
     * {
     * "device_family":"Other",
     * "browser_major":"-",
     * "os_family":"Other",
     * "os_major":"-",
     * "browser_family":"Other",
     * "os_minor":"-"
     * }
     **/
    @Test
    public void testHandlingOfNulls() throws HiveException {

        DeferredJavaObject ua1 = new DeferredJavaObject(null);

        DeferredObject[] args = { ua1 };

        HashMap<String, String> evaled = (HashMap<String, String>) getUAPropertiesUDF
                .evaluate(args);

        String resultOSName = evaled.get("os_family");
        String resultBrowserName = evaled.get("browser_family");
        String resultOsMinor = evaled.get("os_minor");
        assertEquals("OS name check", (new String("Other")),
                resultOSName.toString());
        assertEquals("Browser name check", (new String("Other")),
                resultBrowserName.toString());

        assertEquals("OS minor", (new String("-")),
                resultOsMinor.toString());

    }

}
