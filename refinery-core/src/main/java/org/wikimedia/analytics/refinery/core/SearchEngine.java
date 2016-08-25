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
 * An enum for categorizing search engines in ExternalSearch, which should
 * make it trivial to add new search engines to check in identifySearchEngine
 * since it iterates through all the entries. The name property allows user
 * to specify a string with spaces and symbols if needed.
 */
public enum SearchEngine {
    GOOGLE("Google", "\\.?google\\."),
    YAHOO("Yahoo", "search\\.yahoo\\."),
    BING("Bing", "\\.bing\\."),
    YANDEX("Yandex", "yandex\\."),
    BAIDU("Baidu", "\\.baidu\\."),
    DDG("DuckDuckGo", "\\.?duckduckgo\\.");

    private final String searchEngineName;
    private final Pattern pattern;

    SearchEngine(String name, String regex) {
        this.searchEngineName = name;
        this.pattern = Pattern.compile(regex);
    }

    public String getSearchEngineName() {
        return this.searchEngineName;
    }

    public Pattern getPattern() {
        return this.pattern;
    }
}
