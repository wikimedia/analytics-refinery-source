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


import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestSearchRequest {
    private final String what;
    private final  String uriPath;
    private final  String uriQuery;
    private final  String expected;
    private final boolean searchAPI;

    public TestSearchRequest(String what, String uriPath, String uriQuery, String expected, boolean searchAPI) {
        super();
        this.what = what;
        this.uriPath = uriPath;
        this.uriQuery = uriQuery;
        this.expected = expected;
        this.searchAPI = searchAPI;
    }

    @Parameters
    public static Collection<Object[]> getData() {
        // Page, URI PARAMS, expected classifySearchRequest, isSearchRequest
        return Arrays.asList(new Object[][]{
            {"normal page", "/wiki/Foobarbaz", "", "", false},
            {"random api", "w/api.php", "maxlag=5&format=json&meta=userinfo&action=query", "", false},
            {"search api via list", "w/api.php", "action=query&list=search&srsearch=hosted desktop&srprop=snippet", "cirrus", true},
            {"search api via list (unordered)", "w/api.php", "list=search&action=query&srsearch=hosted desktop&srprop=snippet", "cirrus", true},
            {"morelike api via list", "w/api.php", "action=query&list=search&srsearch=morelike:Hello+World&srprop=snippet", "cirrus (more like)", true},
            {"morelike api via list (unordered)", "w/api.php", "srsearch=morelike:Hello+World&action=query&list=search&srprop=snippet", "cirrus (more like)", true},
            {"search api via generator", "/w/api.php", "action=query&format=json&prop=pageterms%7Cpageimages&wbptterms=description&generator=search&gsrsearch=blah+blah&gsrnamespace=0&gsrwhat=text&gsrinfo=&gsrprop=redirecttitle&gsrlimit=12&piprop=thumbnail&pithumbsize=96&pilimit=12&continue=", "cirrus", true},
            {"morelike api via generator", "/w/api.php", "action=query&format=json&prop=pageterms%7Cpageimages&wbptterms=description&generator=search&gsrsearch=morelike:Hello+World&gsrnamespace=0&gsrwhat=text&gsrinfo=&gsrprop=redirecttitle&gsrlimit=12&piprop=thumbnail&pithumbsize=96&pilimit=12&continue=", "cirrus (more like)", true},
            {"morelike api via generator (unordered)", "/w/api.php", "format=json&prop=pageterms%7Cpageimages&wbptterms=description&gsrsearch=morelike:Hello+World&gsrnamespace=0&action=query&gsrwhat=text&gsrinfo=&gsrprop=redirecttitle&generator=search&gsrlimit=12&piprop=thumbnail&pithumbsize=96&pilimit=12&continue=", "cirrus (more like)", true},
            {"opensearch api", "/w/api.php", "action=opensearch&format=json&search=d1&namespace=0&limit=10", "open", true},
            {"language search", "w/api.php","action=languagesearch&search=espa", "language", true},
            {"geosearch via list", "w/api.php","action=query&list=geosearch&gsradius=10000&gscoord=13.99861|100.53008", "geo", true},
            {"geosearch via generator", "w/api.php","action=query&format=json&prop=coordinates%7Cpageimages%7Cpageterms&colimit=100&piprop=thumbnail&pithumbsize=320&pilimit=100&wbptterms=description&generator=geosearch&ggscoord=12.306473%7C10.254717&ggsradius=520.3277496558758&ggslimit=100&continue=", "geo", true},
            {"prefix via list", "/w/api.php","action=query&format=json&generator=prefixsearch&list=prefixsearch&pssearch=O", "prefix", true},
            {"prefix via list (unordered)", "/w/api.php","format=json&list=prefixsearch&generator=prefixsearch&action=query&pssearch=O", "prefix", true},
            {"prefix via generator", "w/api.php","action=query&format=json&prop=pageprops%7Cpageprops%7Cpageimages%7Cpageterms&generator=prefixsearch&ppprop=displaytitle&piprop=thumbnail&pithumbsize=80&pilimit=15&wbptterms=description&redirects=&gpssearch=blah+blah%C4%87&gpsnamespace=0&gpslimit=15", "prefix", true},
            {"prefix via generator (unordered)", "w/api.php","gpssearch=blah+blah%C4%87&format=json&prop=pageprops%7Cpageprops%7Cpageimages%7Cpageterms&generator=prefixsearch&action=query&ppprop=displaytitle&piprop=thumbnail&pithumbsize=80&pilimit=15&wbptterms=description&redirects=&gpsnamespace=0&gpslimit=15", "prefix", true}
        });
    }

    @Test
    public void testSearchClassifier() {
        String actual = SearchRequest.getInstance().classifySearchRequest(uriPath, uriQuery);
        Assert.assertEquals(what + " (classifySearchRequest)", expected, actual);
        Assert.assertEquals(what + " (isSearch)", searchAPI, SearchRequest.getInstance().isSearchRequest(uriPath, uriQuery));
    }
}
