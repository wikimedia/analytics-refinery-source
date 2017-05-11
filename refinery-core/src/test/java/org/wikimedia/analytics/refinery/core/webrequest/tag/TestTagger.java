package org.wikimedia.analytics.refinery.core.webrequest.tag;

import junit.framework.TestCase;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;
import java.util.Set;

/**
 * Created by nuriaruiz on 5/9/17.
 *
 */
@RunWith(JUnitParamsRunner.class)
public class TestTagger extends TestCase {


    /*
    * Make sure chain was initialized
    */
    @Test
    public void testChainSize() throws Exception {

        TaggerChain taggerChain = new TaggerChain();
        // see faketagger just for tests
        assertTrue(taggerChain.chain.size() == 2);
        assertTrue(taggerChain.chain.get(0) != null);
        assertTrue(taggerChain.chain.get(1) != null);


    }


    /**
     * No tags returns empty set
     * @throws Exception
     */
    @Test
    public void testNoTags() throws Exception{

        TaggerChain taggerChain = new TaggerChain();
        WebrequestData data = new WebrequestData("en.wikipedia","/", "", "200",
            "text/html", "fake user agent", "");

        // not tags thus far
        assertTrue(taggerChain.getTags(data).isEmpty());

    }


    /**
     * Test portal tag
     * @throws Exception
     */
    @Test
    public void testPortalHappyCase() throws Exception{

        TaggerChain taggerChain = new TaggerChain();
        WebrequestData data = new WebrequestData("www.wikipedia.org","/", "", "200",
            "text/html", "fake user agent", "");

        assertTrue(taggerChain.getTags(data).size() == 1);
        assertTrue(taggerChain.getTags(data).contains("portal"));

    }

    @Test
    @FileParameters(
        value = "src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testIsTaggedPageview(
        String test_description,
        String project,
        String dialect,
        String page_title,
        boolean is_pageview,
        boolean is_legacy_pageview,
        String ip_address,
        String x_forwarded_for,
        String uri_host,
        String uri_path,
        String uri_query,
        String http_status,
        String content_type,
        String user_agent,
        String x_analytics_header
    ) throws Exception {
        //uses pageview data to see if a possible pageview tag is behaving as it should
        PageviewDefinition PageviewDefinitionInstance = PageviewDefinition.getInstance();

        WebrequestData data = new WebrequestData(uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            x_analytics_header) ;

        TaggerChain taggerChain = new TaggerChain();

        Set<String> tags = taggerChain.getTags(data);

        // if this a pageview we should have at least 1 tag: 'pageview'


        if (is_pageview) {
            assertTrue(test_description, tags.contains("pageview"));
        } else {
            assertFalse(test_description, tags.contains("pageview"));
        }

    }






}