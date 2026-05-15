// Copyright 2026 Wikimedia Foundation
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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Normalizes {@code wprov} values embedded in the {@code X-Analytics} header string before
 * {@code str_to_map} parsing. Client-side bugs occasionally concatenate URL fragments, percent
 * escapes, or paths into {@code wprov}, which breaks exact-match analytics
 * (see <a href="https://phabricator.wikimedia.org/T425787">T425787</a>).
 * <p>
 * Reserved codes and naming intent are documented on Wikitech
 * <a href="https://wikitech.wikimedia.org/wiki/Provenance">Provenance</a>. The documented URL
 * parameter shape is {@code ?wprov=<3_char_feature><platform_one_char><major_version_uint>}, with
 * letters and digits only in reserved examples. This sanitizer truncates embedded {@code http}
 * fragments, then keeps the leading lowercase ASCII alphanumeric run (so punctuation, {@code _},
 * {@code %}, etc. all stop the prefix the same way).
 */
@ThreadSafe
public final class XAnalyticsWprovSanitizer {

    private static final Pattern WPROV_PAIR = Pattern.compile(
            "(^|;)\\s*([wW][pP][rR][oO][vV])\\s*=\\s*([^;]*)");

    private XAnalyticsWprovSanitizer() {
    }

    /**
     * Rewrites every {@code wprov=...} pair in the header, leaving other keys untouched.
     *
     * @param xAnalytics raw {@code X-Analytics} header text, or {@code null}
     * @return sanitized header, or {@code null} when input is {@code null}
     */
    public static String sanitize(String xAnalytics) {
        if (xAnalytics == null) {
            return null;
        }
        Matcher matcher = WPROV_PAIR.matcher(xAnalytics);
        StringBuffer out = new StringBuffer();
        while (matcher.find()) {
            String prefix = matcher.group(1);
            String key = matcher.group(2);
            String rawValue = matcher.group(3);
            String normalized = normalizeWprovValue(rawValue);
            matcher.appendReplacement(out, Matcher.quoteReplacement(prefix + key + "=" + normalized));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    /**
     * Truncates at embedded {@code http} (see {@link #sanitize} rationale), then keeps only the
     * leading {@code [a-z0-9]+} run. Any {@code %}, {@code /}, {@code _}, punctuation, etc. end the
     * run, so the same logic replaces a separate delimiter-stripping pass.
     */
    static String normalizeWprovValue(String raw) {
        if (raw == null) {
            return null;
        }
        String v = raw.trim();
        if (v.isEmpty()) {
            return v;
        }
        String lower = v.toLowerCase(Locale.ROOT);
        int cut = v.length();
        int httpish = lower.indexOf("http");
        if (httpish > 0) {
            cut = Math.min(cut, httpish);
        }
        v = v.substring(0, cut);
        int end = 0;
        while (end < v.length()) {
            char c = v.charAt(end);
            boolean codeChar = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
            if (!codeChar) {
                break;
            }
            end++;
        }
        return v.substring(0, end);
    }
}
