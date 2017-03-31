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

import java.util.regex.Pattern;

/**
 * An enum of regular expressions for categorizing search API requests.
 * <p>
 * This enum contains the regex patterns and names of search-related
 * APIs that we support (e.g. prefix, full-text, geo, open, language).
 */
public enum SearchRequestApiRegex {

    GEOSEARCH("geo", "(list|generator)=geosearch"),
    OPENSEARCH("open", "action=opensearch"),
    LANGUAGESEARCH("language", "action=languagesearch"),
    MORELIKE("cirrus (more like)", "(?=.*(action=query))(?=.*((list|generator)=search))(?=.*((gsr|sr)search=morelike))"),
    PREFIX("prefix", "(?=.*(action=query))(?=.*((list|generator)=prefixsearch))"),
    FULLTEXT("cirrus", "(?=.*(action=query))(?=.*((list|generator)=search))");

    private final String label;
    private final Pattern pattern;

    SearchRequestApiRegex(String lab, String regex) {
        this.label = lab;
        this.pattern = Pattern.compile(regex);
    }

    public String getLabel() {
        return this.label;
    }

    public Pattern getPattern() {
        return this.pattern;
    }

}
