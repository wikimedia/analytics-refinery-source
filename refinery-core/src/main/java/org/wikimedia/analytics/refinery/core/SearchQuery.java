/**
 * Copyright (C) 2015 Wikimedia Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.core;

import java.util.ArrayList;

/**
 * Functions for working with Cirrus search queries.
 * <p>
 * Deconstruction (feature detection) includes:
 * - Enumerating through the SearchQueryFeature's
 * - Counting the number of double quotes, if any
 */
public class SearchQuery {

    private static final SearchQuery instance = new SearchQuery();

    private SearchQuery() {
    }

    public static SearchQuery getInstance() {
        return instance;
    };

    /**
     * Deconstruct a search query into an array of features.
     * @param searchQuery A string to deconstruct into features.
     * @return "[feature1, ..., featureN]"
     */
    public String deconstructSearchQuery(String searchQuery) {

        ArrayList<String> features = new ArrayList<>();

        if (searchQuery == null) {
            features.add("is null");
            return features.toString();
        }

        /* Test for simple cases: */
        switch(searchQuery) {
            case "":
                features.add("is empty");
                break;
            case "*":
                features.add("is just *");
                break;
            case "?":
                features.add("is just ?");
                break;
            case "~":
                features.add("is just ~");
                break;
            case "searchTerms": // Unresolved Cirrus bug (https://phabricator.wikimedia.org/T102277)
                features.add("is searchTerms");
                break;
        }
        if (features.size() > 0) {
            /* Finish here since there's nothing more we can do with the query. */
            return features.toString();
        }

        /* Non-regex checks: */
        if (searchQuery.contains("@")) {
            features.add("has @");
        }
        if (searchQuery.contains("prefix:")) {
            features.add("is prefix");
        }
        if (searchQuery.contains("insource:")) {
            features.add("is insource");
        }
        if (searchQuery.contains("incategory:")) {
            features.add("is incategory");
        }
        if (searchQuery.contains("intitle:")) {
            features.add("is intitle");
        }
        if (searchQuery.substring(0, 1).equals("~")) {
            features.add("forces search results");
        }
        if (searchQuery.endsWith("?")) {
            features.add("ends with ?");
        }

        /* Search the query for feature regex's: */
        for (SearchQueryFeatureRegex f : SearchQueryFeatureRegex.values()) {
            if (f.getPattern().matcher(searchQuery).find()) {
                features.add(f.getDescription());
            }
        }

        /* Count how many double quotes there are: */
        int doubleQuotesCount = searchQuery.length() - searchQuery.replace("\"", "").length();
        if (doubleQuotesCount > 0) {
            if (doubleQuotesCount == 1) {
                features.add("has one double quote");
            }
            features.add((doubleQuotesCount % 2) == 0 ? "has even double quotes" : "has odd double quotes");
        }

        if (features.size() == 0) {
            features.add("is simple");
        }

        return features.toString();
    }

}
