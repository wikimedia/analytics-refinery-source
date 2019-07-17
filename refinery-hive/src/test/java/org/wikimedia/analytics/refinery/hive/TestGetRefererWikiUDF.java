package org.wikimedia.analytics.refinery.hive;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGetRefererWikiUDF {
    @Test
    public void testNoReferer () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals(null, udf.evaluate("-"));
    }

    @Test
    public void testNullReferer () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals(null, udf.evaluate(null));
    }

    @Test
    public void testEmptyReferer () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals(null, udf.evaluate(""));
    }

    @Test
    public void testExternalReferer () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals(null, udf.evaluate("wololo.com/jajajaja"));
    }

    @Test
    public void testWikipedia () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals("en.wikipedia", udf.evaluate("https://en.wikipedia.org/wiki/The_12_Greek_gods_of_olympus"));
    }

    @Test
    public void testMobileWikipedia () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals("ru.wikipedia", udf.evaluate("https://ru.m.wikipedia.org/wiki/%D0%90%D0%BC%D1%81%D1%82%D0%B5%D1%80%D0%B4%D0%B0%D0%BC"));
    }

    @Test
    public void testCommons () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals("commons.wikimedia", udf.evaluate("https://commons.wikimedia.org/"));
    }

    @Test
    public void testNoLanguage () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals("wikipedia", udf.evaluate("https://www.wikipedia.org/"));
    }

    @Test
    public void testWikiImitation () {
        GetRefererWikiUDF udf = new GetRefererWikiUDF();
        assertEquals(null, udf.evaluate("https://fr-9c90707c28305eb7a.getsmartling.com/wiki/Main_Page"));
    }
}
