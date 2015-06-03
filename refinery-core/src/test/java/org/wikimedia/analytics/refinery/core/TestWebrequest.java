package org.wikimedia.analytics.refinery.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;

import java.util.List;

@RunWith(JUnitParamsRunner.class)
public class TestWebrequest {

    @Test
    @FileParameters(
        value = "src/test/resources/isCrawler_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )

    public void testisCrawler(
        String test_description,
        boolean is_crawler,
        String user_agent
    ) {
        Webrequest webrequest_inst = Webrequest.getInstance();
        assertEquals(
            test_description,
            is_crawler,
            webrequest_inst.isCrawler(
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
        String x_analytics,
        String param
    ) {
        Webrequest instance = Webrequest.getInstance();
        assertEquals(
            test_description,
            expected_output,
            instance.getXAnalyticsValue(
                x_analytics,
                param
            )
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
        Webrequest webrequest_inst = Webrequest.getInstance();
        assertEquals(
            test_description,
            expected_method,
            webrequest_inst.getAccessMethod(
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
        Webrequest webrequest_inst = Webrequest.getInstance();
        assertEquals(
                test_description + " - Project Class",
                expectedProjectClass,
                webrequest_inst.normalizeHost(uriHost).getProjectClass()
        );
        assertEquals(
                test_description + " - Project",
                expectedProject,
                webrequest_inst.normalizeHost(uriHost).getProject()
        );
        assertEquals(
                test_description + " - Qualifiers",
                expectedQualifiers,
                join(webrequest_inst.normalizeHost(uriHost).getQualifiers(), ";")
        );
        assertEquals(
                test_description + " - TLD",
                expectedTld,
                webrequest_inst.normalizeHost(uriHost).getTld()
        );
    }
}