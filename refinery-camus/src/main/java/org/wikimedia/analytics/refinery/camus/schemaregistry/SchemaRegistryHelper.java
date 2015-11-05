package org.wikimedia.analytics.refinery.camus.schemaregistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by nuriaruiz on 11/19/15.
 *
 * Helper functions for schema registry
 */
public class SchemaRegistryHelper {

    //store schema per topic so as to do parsing once
    static Map<String, String> cache = new HashMap<String,String>();

    public static String getSchemaNameFromTopic(String topicName) {
        String schemaName = cache.get(topicName);

        if (schemaName == null) {
            if (topicName.split("_").length >= 2) {
                Integer pos = topicName.indexOf('_');
                schemaName = topicName.substring(pos + 1);
            }
            cache.put(topicName,schemaName);

        }

        if (schemaName == null) {
            throw new RuntimeException(
                "Topic name doesn't conform to prefix_Schema convention");
        }
        return schemaName;
    }
}
