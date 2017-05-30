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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Static functions to work with Wikimedia data, broadly construed;
 * this is where we put the functions generic enough to be reused
 * over and over again in other classes.
 */
public class Utilities {

    /**
     * Check if the target is contained within string.  This is
     * just a convenience method that also makes sure that arguments are not null.
     *
     * @param   string    String to search in
     * @param   target    String to search for
     * @return  boolean
     */
    public static boolean stringContains(String string, String target){
        return (target != null && string != null && string.contains(target));
    }

    /**
     * Convenience method for Using Matcher.find() to check if
     * the given regex Pattern matches the target String.
     * Also called in the LegacyPageview class.
     *
     * @param Pattern pattern
     * @param String  target
     *
     * @return boolean
     */
    public static boolean patternIsFound(Pattern pattern, String target) {
        return pattern.matcher(target).find();
    }

    /**
     * Given a String in the form:
     * key=value;key=value
     *
     *  return the
     * value associated with said key, or an empty string if the key
     * is not found.
     *
     * @param text string with key value pairs separated by ";"
     * @param key the key to search for the value of.
     * @return String
     */
    public static String getValueForKey(String text, String key) {

        String value = "";

        int keyIndex = text.indexOf(key);
        if(keyIndex == -1){
            return value;
        }

        int delimiterIndex = text.indexOf(";", keyIndex);
        if(delimiterIndex == -1){
            value = text.substring(keyIndex + key.length() + 1);
        } else {
            value = text.substring(keyIndex + key.length() + 1, delimiterIndex);
        }

        //Done
        return value;
    }

    /**
     * Implementation is based on LinkedHashMap as this class
     * has the handy ability of reordering records upon record access
     * thus an LRU cache just needs to remove last entry.
     * @param <K>
     * @param <V>
     */
    public static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private int cacheSize;

        public LRUCache(int cacheSize) {
            // Constructs an empty LinkedHashMap instance with the
            // specified initial capacity, load factor and ordering mode.
            super(16, 0.75f, true);
            this.cacheSize = cacheSize;
        }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }


}

}