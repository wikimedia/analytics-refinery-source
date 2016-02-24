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
 * An enum of regular expressions for deconstructing queries.
 * <p>
 * This enum contains the regex patterns and names of features
 * we are interested in deconstructing a search query into.
 */
public enum SearchQueryFeatureRegex {

    AND_OPERATOR("has AND", "\\bAND\\b"),
    OR_OPERATOR("has OR", "\\bOR\\b"),
    NOT_OPERATOR("has NOT", "\\bNOT\\b"),
    MINUS_MODIFIER("has logic inversion (-)", "(\\s|^)-(\\b|['\\\"])"),
    EXCLAIM_MODIFIER("has logic inversion (!)", "(\\s|^)!(\\b|['\\\"])"),
    WILDCARDS("has wildcard", "([\\?\\*]{1}[^\\?\\*]+)|([^\\?\\*]+[\\?\\*]{1})"),
    FUZZY_SEARCH("is fuzzy search", ".+\\~[0-9]*$"),
    INSOURCE_REGEX("is insource (regex)", "insource:/.+/i?$"),
    QUOT("has quot", "(\\bquot\\b)"),
    ALL_PUNCTUATION("is only punctuation and spaces", "^[\\p{Punct}\\p{Blank}&&[^\\*\\?~]]+$"),
    NON_ASCII("has non-ASCII", "[^0-9\\p{ASCII}]+");

    private final String description;
    private final Pattern pattern;

    SearchQueryFeatureRegex(String desc, String regex) {
        this.description = desc;
        this.pattern = Pattern.compile(regex);
    }

    public String getDescription() {
        return this.description;
    }

    public Pattern getPattern() {
        return this.pattern;
    }

}
