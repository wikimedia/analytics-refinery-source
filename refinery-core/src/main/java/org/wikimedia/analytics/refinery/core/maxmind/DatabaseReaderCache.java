/**
 * Copyright (C) 2020 Wikimedia Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.core.maxmind;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.NodeCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;

public class DatabaseReaderCache implements NodeCache {
    private final Cache<Integer, JsonNode> cache;

    /**
     * This initializer adds a Caffeine cache to the MaxMind DB reader.
     *
     * This cache is limited in size. [[https://github.com/ben-manes/caffeine]]
     *
     * @param cacheSize The maximum number of item in the cache
     */
    public DatabaseReaderCache(int cacheSize) {
        cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    public final JsonNode get(int key, Loader loader) throws IOException {
        // NOTE: We use the getIfPresent/put strategy instead of the
        // cache.get(key, loadingMethod) one as the latter fails on our unit-tests,
        // leading to a never-ending loop when managing the cache.
        //
        // The reason is Maxmind can do recursive calls to the cache which is explicitly not supported by Caffeine.
        // https://github.com/maxmind/MaxMind-DB-Reader-java/blob/main/src/main/java/com/maxmind/db/Decoder.java#L116
        // https://github.com/ben-manes/caffeine/wiki/Faq#recursive-computations
        //
        JsonNode result = cache.getIfPresent(key);
        if (result == null) {
            result = loader.load(key);
            cache.put(key, result);
        }
        return result;
    }
}
