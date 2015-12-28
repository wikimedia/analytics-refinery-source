/**
 * Copyright (C) 2015  Wikimedia Foundation
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

import java.util.regex.Pattern;

/**
 * Functions to identify traffic from external search engines
 */
public class SearchEngineClassifier {

    /*
     * Meta-methods to enable eager instantiation in a singleton-based way.
     */
    private static final SearchEngineClassifier instance = new SearchEngineClassifier();

    private SearchEngineClassifier() {
    }

    public static SearchEngineClassifier getInstance() {
        return instance;
    }

    /*
     * A simple pattern for search identification
     */
    private static final Pattern searchPattern = Pattern.compile("(\\.(baidu|bing|google)|search\\.yahoo|yandex)\\.");

    /**
     * Crudely subsets a referer to just contain the domain,
     * rather than the path or scheme.
     *
     * @param referer the value in the referer field.
     * @return String
     */
    private String refererSubstring(String referer) {

        int methodLocation = referer.indexOf("://");
        int pathLocation;
        if (methodLocation == -1) {
            pathLocation = referer.indexOf("/");
        } else {
            pathLocation = referer.indexOf("/", methodLocation + 3);
        }
        if (pathLocation > -1) {
            return referer.substring(0, pathLocation);
        }
        return referer;

    }

    /**
     * Determines whether a request was sent from a search
     * engine, based on the referer.
     *
     * @param referer the value in the referer field.
     * @return boolean
     */
    public boolean isExternalSearch(String referer) {
        return searchPattern.matcher(refererSubstring(referer)).find();
    }

    /**
     * Provides a simple classification of requests based
     * on their referer, with options of "none", "search engine"
     * and "other".
     *
     * @param referer the value in the referer field.
     * @return String
     */
    public String refererClassify(String referer) {

        String ref_class = Webrequest.getInstance().classifyReferer(referer);

        if (ref_class.equals("external")) {
            if (isExternalSearch(referer)) {
                return Referer.SEARCH_ENGINE.getRefLabel();
            }
        }

        return ref_class;

    }

    /**
     * Determines the search engine that served as a referer
     * for a particular request. Options are "None" where
     * the request fails to pass isExternalSearch, "Unknown",
     * where no heuristic is passed, and otherwise one of
     * "Google", "Bing", "Yandex", "Yahoo" or "Baidu".
     *
     * @param referer the value in the referer field.
     * @return String
     */
    public String identifySearchEngine(String referer) {

        if (!refererClassify(referer).equals("external (search engine)")) {
            return Referer.NONE.getRefLabel();
        }

        String filteredReferer = refererSubstring(referer);

        for (SearchEngine se : SearchEngine.values()) {
            if (se.getPattern().matcher(filteredReferer).find()) {
                return se.getSearchEngineName();
            }
        }

        /* Used in the really odd case that the referer passes isExternalSearch
         * check but a specific search engine is not identified. May happen if
         * searchPattern is updated, but SearchEngine enum is not.
         */
        return Referer.UNKNOWN.getRefLabel();

    }

}
