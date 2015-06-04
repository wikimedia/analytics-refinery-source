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

public class TestSearchRequest extends TestCase {

    //Generic classifier assertion
    private void assertSearchClassifier(final String uriPath, final String uriQuery, final String expected) {
        String actual = SearchRequest.getInstance().classifySearchRequest(uriPath, uriQuery);

        assertEquals("The actual output does not match the expected output",
                expected, actual);
    }

    //Test a normal request. Expect an empty string from the search classifer.
    public void testNoneClassify() {
        assertSearchClassifier("/wiki/Foobarbaz", "", "");
    }

    //Test an API request lacking any of the right action=foo entries.
    public void testAPINoActionClassify() {
        assertSearchClassifier("w/api.php","maxlag=5&format=json&meta=userinfo&action=query", "");
    }

    //Test an API request with an acceptable action but no search entry
    public void testAPINoSearchClassify() {
        assertSearchClassifier("w/api.php","action=query&prop=revisions&titles=hall&rvprop=content", "");
    }

    //Test a Cirrus Search request. Expect "cirrus"
    public void testCirrusClassify() {
        assertSearchClassifier("w/api.php","action=query&list=search&srsearch=hosted desktop&srprop=snippet", "cirrus");
    }

    //Test a Open Search request. Expect "open"
    public void testOpenClassify() {
        assertSearchClassifier("w/api.php","action=opensearch&format=json&search=d1&namespace=0&limit=10", "open");
    }

    //Test a Language Search request. Expect "language"
    public void testLanguageClassify() {
        assertSearchClassifier("w/api.php","action=languagesearch&search=espa", "language");
    }

    //Test a Geo Search request. Expect "geo"
    public void testGeoClassify() {
        assertSearchClassifier("w/api.php","action=query&list=geosearch&gsradius=10000&gscoord=13.99861|100.53008", "geo");
    }

    //Test a Prefix Search request. Expect "prefix"
    public void testPrefixClassify() {
        assertSearchClassifier("w/api.php","action=query&format=json&generator=prefixsearch&list=prefixsearch&pssearch=O", "prefix");
    }

    //Generic boolean assertion
    private void assertSearchBoolean(final String uriPath, final String uriQuery, final boolean expected) {
        boolean actual = SearchRequest.getInstance().isSearchRequest(uriPath, uriQuery);

        assertEquals("The actual output does not match the expected output",
                expected, actual);
    }

    //Test a normal request. Expect false.
    public void testNoneBoolean() {
        assertSearchBoolean("/wiki/Foobarbaz", "", false);
    }

    //Test an API request lacking any of the right action=foo entries. Expect false.
    public void testAPINoActionBoolean() {
        assertSearchBoolean("w/api.php","maxlag=5&format=json&meta=userinfo&action=query", false);
    }

    //Test an API request with an acceptable action but no search entry. Expect false.
    public void testAPINoSearchBoolean() {
        assertSearchBoolean("w/api.php","action=query&prop=revisions&titles=hall&rvprop=content", false);
    }

    //Test a Cirrus Search request. Expect true
    public void testCirrusBoolean() {
        assertSearchBoolean("w/api.php","action=query&list=search&srsearch=hosted desktop&srprop=snippet", true);
    }

    //Test a Open Search request. Expect true
    public void testOpenBoolean() {
        assertSearchBoolean("w/api.php","action=opensearch&format=json&search=d1&namespace=0&limit=10", true);
    }

    //Test a Language Search request. Expect true
    public void testLanguageBoolean() {
        assertSearchBoolean("w/api.php","action=languagesearch&search=espa", true);
    }

    //Test a Geo Search request. Expect true
    public void testGeoBoolean() {
        assertSearchBoolean("w/api.php","action=query&list=geosearch&gsradius=10000&gscoord=13.99861|100.53008", true);
    }

    //Test a Prefix Search request. Expect true
    public void testPrefixBoolean() {
        assertSearchBoolean("w/api.php","action=query&format=json&generator=prefixsearch&list=prefixsearch&pssearch=O", true);
    }
}
