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

import com.maxmind.db.CHMCache;
import com.maxmind.db.NodeCache;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class AbstractDatabaseReader implements Serializable {

    public static final int MAXMIND_LRU_CACHE_SIZE = 10000;

    protected abstract Logger getLogger();

    public abstract String getDefaultDatabasePathPropertyName();
    public abstract String getDefaultDatabasePath();

    protected transient DatabaseReader reader;
    protected String databasePath;
    protected NodeCache cache;

    /**
     * Method initializing the maxmind reader
     * @throws IOException
     */
    public void initializeReader() throws IOException {

        String checkedDatabasePath = this.databasePath;

        if (checkedDatabasePath == null || checkedDatabasePath.isEmpty()) {
            checkedDatabasePath = System.getProperty(getDefaultDatabasePathPropertyName(), getDefaultDatabasePath());
        }
        getLogger().info("Geocode using MaxMind database: " + checkedDatabasePath);

        if (this.cache == null) {
            cache = new LRUCache(MAXMIND_LRU_CACHE_SIZE);
        }

        this.reader = new DatabaseReader.Builder(new File(checkedDatabasePath)).withCache(cache).build();

    }

    /**
     * Provide the Serialize readObject method to enforce MaxmindReader initialization at deserialization
     */
    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
        // Use the default deserialize method
        aInputStream.defaultReadObject();
        // Initialize the reader as it is transient
        initializeReader();
    }

}