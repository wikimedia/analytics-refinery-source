package org.wikimedia.analytics.refinery.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;

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
        assertEquals(
            test_description,
            is_crawler,
            Webrequest.isCrawler(
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
        assertEquals(
            test_description,
            expected_output,
            Webrequest.getXAnalyticsValue(
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

        assertEquals(
            test_description,
            expected_method,
            Webrequest.getAccessMethod(
                uri_host,
                user_agent
            )
        );
    }
}