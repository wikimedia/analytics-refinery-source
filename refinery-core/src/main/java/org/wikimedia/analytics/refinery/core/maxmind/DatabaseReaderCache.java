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
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.maxmind.db.NodeCache;

public class DatabaseReaderCache implements NodeCache {
    private Cache<Integer, JsonNode> cache;

    /**
     * This initializer adds a Guava cache to the MaxMind DB reader.
     * This cache is limited in size. Least recently used items are removed first.
     * [[https://github.com/google/guava/wiki/CachesExplained]]
     *
     * @param cacheSize The maximum number of item in the cache
     */
    public DatabaseReaderCache(int cacheSize) {
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
    }

    public JsonNode get(int key, Loader loader) throws IOException {
        try {
            return cache.get(key, () -> loader.load(key));
        } catch (ExecutionException e) {
            throw new IOException("Failure when loading from Maxmind DatabaseReaderCache", e);
        }
    }
}
