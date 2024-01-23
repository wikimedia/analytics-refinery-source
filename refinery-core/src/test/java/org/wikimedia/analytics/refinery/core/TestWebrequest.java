package org.wikimedia.analytics.refinery.core;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class TestWebrequest {

    private final Webrequest webrequest = Webrequest.getInstance();

    @Deprecated
    @Test
    @FileParameters(
        value = "src/test/resources/isSpider_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )

    public void testIsCrawler(
        String test_description,
        boolean is_crawler,
        String user_agent
    ) {
        assertEquals(
            test_description,
            is_crawler,
            webrequest.isCrawler(
                user_agent
            )
        );
    }

    @Test
    @FileParameters(
        value = "src/test/resources/isSpider_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )

    public void testIsSpider(
        String test_description,
        boolean isSpider,
        String user_agent
    ) {
        assertEquals(
            test_description,
            isSpider,
            webrequest.isSpider(
                user_agent
            )
        );
    }

    @Test
    @FileParameters(
        value = "src/test/resources/x_analytics_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testGetXAnalyticsValues(
        String test_description,
        String expected_output,
        String x_analytics_str,
        String param
    ) throws HiveException {

        WebrequestData data = new WebrequestData("uriHost", "uriPath", "uriQuery",
                "httpStatus", "contentType", "userAgent",
                Utils.parseXAnalyticsHeader(x_analytics_str));

        assertEquals(
            test_description,
            expected_output,
            data.getXAnalyticsHeader().getOrDefault(param, "")
        );
    }

    @Test
    @FileParameters(
        value = "../refinery-core/src/test/resources/access_method_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testGetAccessMethod(
        String test_description,
        String expected_method,
        String uri_host,
        String user_agent
    ) {
        assertEquals(
            test_description,
            expected_method,
            webrequest.getAccessMethod(
                uri_host,
                user_agent
            )
        );
    }

    private String join(List<String> l, String sep) {
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
        String expectedProjectClass,
        String expectedProject,
        String expectedQualifiers,
        String expectedTld,
        String uriHost
    ) {
        assertEquals(
            test_description + " - Project Class",
            expectedProjectClass,
            webrequest.normalizeHost(uriHost).getProjectFamily()
        );
        assertEquals(
            test_description + " - Project",
            expectedProject,
            webrequest.normalizeHost(uriHost).getProject()
        );
        assertEquals(
            test_description + " - Qualifiers",
            expectedQualifiers,
            join(webrequest.normalizeHost(uriHost).getQualifiers(), ";")
        );
        assertEquals(
            test_description + " - TLD",
            expectedTld,
            webrequest.normalizeHost(uriHost).getTld()
        );
    }

    @Test
    public void testNormalizeHostEmpty() {

        String testUriHost = null;

        // Null host
        assertEquals(
            "Null - Project Class",
            NormalizedHostInfo.EMPTY_NORM_HOST_VALUE,
            webrequest.normalizeHost(testUriHost).getProjectFamily()
        );
        assertEquals(
            "Null - Project",
            NormalizedHostInfo.EMPTY_NORM_HOST_VALUE,
            webrequest.normalizeHost(testUriHost).getProject()
        );
        assertEquals(
            "Null - Qualifiers",
            new ArrayList<String>(),
            webrequest.normalizeHost(testUriHost).getQualifiers()
        );
        assertEquals(
            "Null - TLD",
            NormalizedHostInfo.EMPTY_NORM_HOST_VALUE,
            webrequest.normalizeHost(testUriHost).getTld()
        );

        testUriHost = "";

        // Empty host
        assertEquals(
            "Empty - Project Class",
            NormalizedHostInfo.EMPTY_NORM_HOST_VALUE,
            webrequest.normalizeHost(testUriHost).getProjectFamily()
        );
        assertEquals(
            "Empty - Project",
            NormalizedHostInfo.EMPTY_NORM_HOST_VALUE,
            webrequest.normalizeHost(testUriHost).getProject()
        );
        assertEquals(
            "Empty - Qualifiers",
            new ArrayList<String>(),
            webrequest.normalizeHost(testUriHost).getQualifiers()
        );
        assertEquals(
            "Empty - TLD",
            NormalizedHostInfo.EMPTY_NORM_HOST_VALUE,
            webrequest.normalizeHost(testUriHost).getTld()
        );
    }

    @Test
    public void testIsWMFHostname() {

        assertTrue(
            "Should be WMF hostname",
            webrequest.isWMFHostname("en.wikipedia.org")
        );

        assertTrue(
            "Should be WMF hostname",
            webrequest.isWMFHostname("recommend.wmflabs.org")
        );

        assertTrue(
            "Should be WMF hostname",
            webrequest.isWMFHostname("recommend.wmcloud.org")
        );

        assertTrue(
            "Should be WMF hostname",
            webrequest.isWMFHostname("recommend.toolforge.org")
        );

        assertFalse(
            "Should not be WMF hostname",
            webrequest.isWMFHostname("boogers.com")
        );
    }
}