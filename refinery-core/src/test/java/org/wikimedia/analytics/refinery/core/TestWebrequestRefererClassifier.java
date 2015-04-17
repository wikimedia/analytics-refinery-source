// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.core;


import junit.framework.TestCase;

public class TestWebrequestRefererClassifier extends TestCase {

    // Helper methods ---------------------------------------------------------

    private void assertKind(final String url, final String expected) {
        String actual = Webrequest.getInstance().classifyReferer(url);

        assertEquals("Identification output does not match expected",
                expected, actual);
    }

    private void assertUnknown(final String url) {
        assertKind(url, Webrequest.REFERER_UNKNOWN);
    }

    private void assertInternal(final String url) {
        assertKind(url, Webrequest.REFERER_INTERNAL);
    }

    private void assertExternal(final String url) {
        assertKind(url, Webrequest.REFERER_EXTERNAL);
    }

    // Test degernerate settings ----------------------------------------------

    public void testNull() {
        assertUnknown(null);
    }

    public void testEmpty() {
        assertUnknown("");
    }

    public void testDash() {
        assertUnknown("-");
    }

    public void testFoo() {
        assertUnknown("foo");
    }

    public void testFooBar() {
        assertUnknown("foo-bar");
    }

    public void testOrgHttp() {
        assertUnknown("http://org");
    }

    public void testWikipediaOrgWithoutProtocol() {
        assertUnknown("en.wikipedia.org");
    }

    public void testWikipediaOrgDoubleDot() {
        assertUnknown("http://en.wikipedia..org");
    }

    // Test protocols ---------------------------------------------------------

    public void testNodoubleSlash() {
        assertUnknown("https:/foo/en.wikipedia.org");
    }

    public void testWikipediaOrgHttps() {
        assertInternal("https://en.wikipedia.org");
    }

    public void testGoogleHttps() {
        assertExternal("https://www.google.com");
    }

    // Test internal ----------------------------------------------------------

    public void testWikipediaOrgHttp() {
        assertInternal("http://en.wikipedia.org");
    }

    public void testWiktionaryOrg() {
        assertInternal("http://en.wiktionary.org");
    }

    public void testWikibooksOrg() {
        assertInternal("http://en.wikibooks.org");
    }

    public void testWikinewsOrg() {
        assertInternal("http://en.wikinews.org");
    }

    public void testWikiquoteOrg() {
        assertInternal("http://en.wikiquote.org");
    }

    public void testWikisourceOrg() {
        assertInternal("http://en.wikisource.org");
    }

    public void testWikiversityOrg() {
        assertInternal("http://en.wikiversity.org");
    }

    public void testWikivoyageOrg() {
        assertInternal("http://en.wikivoyage.org");
    }

    public void testWikidataOrg() {
        assertInternal("http://www.wikidata.org");
    }

    public void testMediaWikiOrg() {
        assertInternal("http://www.mediawiki.org");
    }

    public void testWikimediaOrg() {
        assertInternal("http://www.mediawiki.org");
    }

    public void testWikimediaFoundationOrg() {
        assertInternal("http://wikimediafoundation.org");
    }

    // Test external ----------------------------------------------------------

    public void testGoogleHttp() {
        assertExternal("http://www.google.com");
    }

    // Test suffix ------------------------------------------------------------

    public void testWikipediaHttpWithSlash() {
        assertInternal("http://en.wikipedia.org/");
    }

    public void testWikipediaHttpWithPath() {
        assertInternal("http://en.wikipedia.org/foo");
    }

    public void testGoogleHttpWithSlash() {
        assertExternal("http://www.google.com/");
    }

    public void testGoogleHttpWithPath() {
        assertExternal("http://www.google.com/foo");
    }


}
