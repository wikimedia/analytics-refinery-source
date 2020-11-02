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

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.NodeCache;
import org.wikimedia.analytics.refinery.core.Utilities;

import java.io.IOException;

public class LRUCache extends Utilities.LRUCache<Integer, JsonNode> implements NodeCache {

    public LRUCache(int cacheSize) {
        super(cacheSize);
    }

    @Override
    public JsonNode get(int key, Loader loader) throws IOException {
        if (this.containsKey(key)) {
            return this.get(key);
        } else {
            JsonNode value = loader.load(key);
            this.put(key, value);
            return value;
        }
    }
}