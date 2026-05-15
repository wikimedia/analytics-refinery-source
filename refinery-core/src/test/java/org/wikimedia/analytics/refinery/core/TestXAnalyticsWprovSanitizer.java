package org.wikimedia.analytics.refinery.core;

import org.junit.Assert;
import org.junit.Test;

public class TestXAnalyticsWprovSanitizer {

    @Test
    public void testNullAndEmptyXAnalyticsPassthrough() {
        Assert.assertNull(XAnalyticsWprovSanitizer.sanitize(null));
        Assert.assertEquals("", XAnalyticsWprovSanitizer.sanitize(""));
    }

    @Test
    public void testNullAndEmptyWprovValue() {
        Assert.assertEquals("", XAnalyticsWprovSanitizer.normalizeWprovValue(""));
        Assert.assertEquals("", XAnalyticsWprovSanitizer.normalizeWprovValue("   "));
        Assert.assertEquals("", XAnalyticsWprovSanitizer.normalizeWprovValue("\t  "));

        Assert.assertEquals("https=1;wprov=;mf-m=b", XAnalyticsWprovSanitizer.sanitize("https=1;wprov=;mf-m=b"));
        Assert.assertEquals("https=1;mf-m=b", XAnalyticsWprovSanitizer.sanitize("https=1;mf-m=b"));
    }

    @Test
    public void testTicketExamples() {
        Assert.assertEquals(
                "https=1;wprov=sfla1;mf-m=b",
                XAnalyticsWprovSanitizer.sanitize(
                        "https=1;wprov=sfla1https://en.wikipedia.org/wiki/Foo;mf-m=b"));
        Assert.assertEquals(
                "wprov=yicw1",
                XAnalyticsWprovSanitizer.sanitize("wprov=yicw1%D8%B4%D9%8A%D8%B1"));
        Assert.assertEquals(
                "wprov=rarw1",
                XAnalyticsWprovSanitizer.sanitize("wprov=rarw1/static/foo.css"));
        Assert.assertEquals(
                "wprov=sfti1",
                XAnalyticsWprovSanitizer.sanitize("wprov=sfti1%23fragment"));
    }

    @Test
    public void testPreservesOtherKeysAndKeyCase() {
        Assert.assertEquals(
                "HTTPS=1;WPROV=wppw1t;page_id=12",
                XAnalyticsWprovSanitizer.sanitize(
                        "HTTPS=1;WPROV=wppw1t;page_id=12"));
    }

    @Test
    public void testMultipleWprovOccurrences() {
        Assert.assertEquals(
                "wprov=wppw1;a=1;wprov=wppw1t",
                XAnalyticsWprovSanitizer.sanitize(
                        "wprov=wppw1https://junk;a=1;wprov=wppw1thttps://x"));
    }

    @Test
    public void testTruncatesAtFirstUnderscore() {
        Assert.assertEquals(
                "wprov=acrw1",
                XAnalyticsWprovSanitizer.sanitize("wprov=acrw1_0?useskin=monobook"));
        Assert.assertEquals(
                "wprov=acrw1",
                XAnalyticsWprovSanitizer.sanitize("wprov=acrw1_-1"));
        Assert.assertEquals(
                "wprov=srpw1",
                XAnalyticsWprovSanitizer.sanitize("wprov=srpw1_dym1"));
        Assert.assertEquals(
                "wprov=srpw1",
                XAnalyticsWprovSanitizer.sanitize("wprov=srpw1_0"));
    }

    @Test
    public void testStripsCommaAndParensGarbage() {
        Assert.assertEquals(
                "wprov=sfla1",
                XAnalyticsWprovSanitizer.sanitize("wprov=sfla1,https://focus.de/259561009"));
        Assert.assertEquals(
                "wprov=sfla1",
                XAnalyticsWprovSanitizer.sanitize("wprov=sfla1)"));
        Assert.assertEquals(
                "wprov=srpw1",
                XAnalyticsWprovSanitizer.sanitize(
                        "wprov=srpw1_3_(%E6%B6%88%E6%AD%A7%E4%B9%89)"));
    }
}
