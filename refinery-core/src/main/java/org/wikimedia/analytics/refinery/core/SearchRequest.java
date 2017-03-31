/**
 * Copyright (C) 2015 Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.core;

/**
 * Functions to work with Wikimedia webrequest data.
 * These functions are optimised for identifying and categorising API requests using the search system.
 */
public class SearchRequest {

    /**
     * Meta-methods to enable eager instantiation in a singleton-based way.
     * in non-Java terms: you get to only create one class instance, and only
     * when you need it, instead of always having everything (static/eager instantiation)
     * or always generating everything anew (!singletons). So we have:
     * (1) an instance;
     * (2) an empty constructor (to avoid people just calling the constructor);
     * (3) an actual getInstance method to allow for instantiation.
     */
    private static final SearchRequest instance = new SearchRequest();

    private SearchRequest() {
    }

    public static SearchRequest getInstance() {
        return instance;
    }

    /**
     * Given a uriPath and uriHost, detect what type of search request
     * a request is. If it doesn't match any, return an empty string
     *
     * @param   uriPath     Path portion of the URI
     * @param   uriQuery    Query portion of the URI
     *
     * @return  string
     */
    public String classifySearchRequest(
        String uriPath,
        String uriQuery
    ) {
        if (uriQuery != null && Utilities.stringContains(uriPath, "api.php")) {
            for (SearchRequestApiRegex api : SearchRequestApiRegex.values()) {
                if (api.getPattern().matcher(uriQuery).find()) {
                    return api.getLabel();
                }
            }
        }

        return ""; // If none of the patterns are found.
    }

    /**
     * Identifies, in a boolean fashion, whether a request was a search
     * request. Doesn't care what /kind/ of request.
     *
     * @param   uriPath     Path portion of the URI
     * @param   uriQuery    Query portion of the URI
     *
     * @return  boolean
     */

    public boolean isSearchRequest(
        String uriPath,
        String uriQuery
    ) {

      return !classifySearchRequest(uriPath, uriQuery).isEmpty();

    }
}
