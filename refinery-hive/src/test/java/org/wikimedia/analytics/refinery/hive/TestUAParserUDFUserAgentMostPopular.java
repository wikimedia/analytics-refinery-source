package org.wikimedia.analytics.refinery.hive;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Collection;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;


/**
 * We test the most popular browser + device combos (from sampled logs)
 * and the ua parser reporting on on those.
 * <p/>
 * Test failing will indicate than the newer version of ua parser
 * is significantly different from the prior one.
 */
@RunWith(Parameterized.class)
public class TestUAParserUDFUserAgentMostPopular {
    ObjectInspector[] initArguments = null;
    UAParserUDF uaParserUDF = null;
    JSONParser parser = null;

    @Before
    public void setUp() throws HiveException {
        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        initArguments = new ObjectInspector[]{valueOI};
        uaParserUDF = new UAParserUDF();
        uaParserUDF.initialize(initArguments);
        parser = new JSONParser();

    }

    @After
    public void tearDown() throws HiveException {

        try {
            uaParserUDF.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"8\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"GMozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36", "{\"os_minor\": \"4\", \"os_major\": \"4\", \"device_family\": \"Samsung GT-I9505\", \"os_family\": \"Android\", \"browser_major\": \"38\", \"browser_family\": \"Chrome Mobile\"}"},
                {"Mozilla/5.0 (compatible; YoudaoBot/1.0; http://www.youdao.com/help/webmaster/spider/; )", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Spider\", \"os_family\": \"Other\", \"browser_major\": \"-\", \"browser_family\": \"Other\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/6.2 Safari/537.85.10", "{\"os_minor\": \"8\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"6\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"36\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) CriOS/38.0.2125.59 Mobile/11D257 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"38\", \"browser_family\": \"Chrome Mobile iOS\"}"},
                {"Opera/9.80 (Windows NT 5.1) Presto/2.12.388 Version/12.17", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows XP\", \"browser_major\": \"12\", \"browser_family\": \"Opera\"}"},
                {"Mozilla/5.0 (Linux; Android 4.4.4; Nexus 5 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36", "{\"os_minor\": \"4\", \"os_major\": \"4\", \"device_family\": \"Nexus 5\", \"os_family\": \"Android\", \"browser_major\": \"38\", \"browser_family\": \"Chrome Mobile\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53", "{\"os_minor\": \"0\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36 OPR/25.0.1614.50", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"25\", \"browser_family\": \"Opera\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 6_1_3 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10B329 Safari/8536.25", "{\"os_minor\": \"1\", \"os_major\": \"6\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"6\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"7\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"35\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 8_0_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) CriOS/38.0.2125.59 Mobile/12A405 Safari/600.1.4", "{\"os_minor\": \"0\", \"os_major\": \"8\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"38\", \"browser_family\": \"Chrome Mobile iOS\"}"},
                {"Mozilla/5.0 (Android; Mobile; rv:33.0) Gecko/33.0 Firefox/33.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Android\", \"browser_major\": \"33\", \"browser_family\": \"Firefox Mobile\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"AppleDictionaryService/208", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Other\", \"browser_major\": \"-\", \"browser_family\": \"Other\"}"},
                {"Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows Vista\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3", "{\"os_minor\": \"1\", \"os_major\": \"5\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"5\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (en-us) AppleWebKit/534.14 (KHTML, like Gecko; Google Wireless Transcoder) Chrome/9.0.597 Safari/534.14", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Other\", \"browser_major\": \"9\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 YaBrowser/14.8.1985.12084 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"14\", \"browser_family\": \"Yandex Browser\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 8_0 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12A365 Safari/600.1.4", "{\"os_minor\": \"0\", \"os_major\": \"8\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"MediaWiki/1.25wmf3", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Other\", \"browser_major\": \"-\", \"browser_family\": \"Other\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"11\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"6\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Opera/9.80 (Windows NT 6.1; WOW64) Presto/2.12.388 Version/12.17", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"12\", \"browser_family\": \"Opera\"}"},
                {"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:33.0) Gecko/20100101 Firefox/33.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Ubuntu\", \"browser_major\": \"33\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 8_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B410 Safari/600.1.4", "{\"os_minor\": \"1\", \"os_major\": \"8\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 7_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D167 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36 OPR/25.0.1614.50", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"25\", \"browser_family\": \"Opera\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B651 Safari/9537.53", "{\"os_minor\": \"0\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", "{\"os_minor\": \"0\", \"os_major\": \"6\", \"device_family\": \"Spider\", \"os_family\": \"iOS\", \"browser_major\": \"2\", \"browser_family\": \"Googlebot\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"31\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/6.1.6 Safari/537.78.2", "{\"os_minor\": \"7\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"6\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 6_1_3 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10B329 Safari/8536.25", "{\"os_minor\": \"1\", \"os_major\": \"6\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"6\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8\", \"browser_major\": \"10\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a Safari/9537.53", "{\"os_minor\": \"0\", \"os_major\": \"7\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 YaBrowser/14.8.1985.12084 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"14\", \"browser_family\": \"Yandex Browser\"}"},
                {"Mozilla/5.0 (Windows NT 6.0; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows Vista\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 8_0_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/4.2.2.38484 Mobile/12A405 Safari/9537.53", "{\"os_minor\": \"0\", \"os_major\": \"8\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"7\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"7\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/4.2.2.38484 Mobile/11D257 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows Vista\", \"browser_major\": \"9\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (Windows NT 6.2; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Spider\", \"os_family\": \"Other\", \"browser_major\": \"2\", \"browser_family\": \"Googlebot\"}"},
                {"NativeHost", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Other\", \"browser_major\": \"-\", \"browser_family\": \"Other\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"7\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36 OPR/25.0.1614.50", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"25\", \"browser_family\": \"Opera\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"33\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D201 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.59.10 (KHTML, like Gecko) Version/5.1.9 Safari/534.59.10", "{\"os_minor\": \"6\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"5\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"10\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"10\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D167 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows XP\", \"browser_major\": \"33\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows Vista\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Other\", \"browser_major\": \"1\", \"browser_family\": \"FacebookBot\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"33\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12A365 Safari/600.1.4", "{\"os_minor\": \"0\", \"os_major\": \"8\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a Safari/9537.53", "{\"os_minor\": \"0\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 8_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B411 Safari/600.1.4", "{\"os_minor\": \"1\", \"os_major\": \"8\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"10\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows XP\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"-", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Other\", \"browser_major\": \"-\", \"browser_family\": \"Other\"}"},
                {"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Spider\", \"os_family\": \"Other\", \"browser_major\": \"2\", \"browser_family\": \"bingbot\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"11\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25", "{\"os_minor\": \"10\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"8\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"9\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D201 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10", "{\"os_minor\": \"9\", \"os_major\": \"10\", \"device_family\": \"Other\", \"os_family\": \"Mac OS X\", \"browser_major\": \"7\", \"browser_family\": \"Safari\"}"},
                {"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"9\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 8_0_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12A405 Safari/600.1.4", "{\"os_minor\": \"0\", \"os_major\": \"8\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPad\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows XP\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"33\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"11\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows XP\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"37\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 8_0_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12A405 Safari/600.1.4", "{\"os_minor\": \"0\", \"os_major\": \"8\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"8\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 8.1\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53", "{\"os_minor\": \"1\", \"os_major\": \"7\", \"device_family\": \"iPhone\", \"os_family\": \"iOS\", \"browser_major\": \"7\", \"browser_family\": \"Mobile Safari\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"11\", \"browser_family\": \"IE\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"32\", \"browser_family\": \"Firefox\"}"},
                {"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},
                {"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36", "{\"os_minor\": \"-\", \"os_major\": \"-\", \"device_family\": \"Other\", \"os_family\": \"Windows 7\", \"browser_major\": \"38\", \"browser_family\": \"Chrome\"}"},

        });
    }

    private String fInput;

    private String fExpected;


    public TestUAParserUDFUserAgentMostPopular(String input, String expected) {
        fInput = input;
        fExpected = expected;
    }


    @Test
    public void testMatchingOfMostPopularUA() throws HiveException, ParseException {

        DeferredJavaObject ua = new DeferredJavaObject(fInput);

        // decode expected output and turn it into an object
        Object obj = parser.parse(fExpected);
        JSONObject expected_ua = (JSONObject) obj;

        DeferredObject[] args1 = {ua};
        HashMap<String, String> computed_ua = (HashMap<String, String>) uaParserUDF
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



