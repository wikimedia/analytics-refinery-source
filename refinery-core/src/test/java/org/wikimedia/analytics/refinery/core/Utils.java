package org.wikimedia.analytics.refinery.core;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {

    public static Map<String, String> parseXAnalyticsHeader(String xAnalyticsHeaderStr) {
        if (xAnalyticsHeaderStr == null || xAnalyticsHeaderStr.isEmpty()) {
            return java.util.Collections.emptyMap();
        }
        return Arrays.stream(xAnalyticsHeaderStr.split(";"))
            .map(pair -> pair.split("=", 2))
            .filter(parts -> parts.length == 2)
            .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
    }
}
