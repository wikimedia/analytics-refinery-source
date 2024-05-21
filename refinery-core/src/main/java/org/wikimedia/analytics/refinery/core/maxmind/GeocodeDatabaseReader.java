/**
 * Copyright (C) 2014 Wikimedia Foundation
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

import static org.wikimedia.analytics.refinery.core.maxmind.RefineryGeocodeDatabaseResponse.UNKNOWN_GEOCODE;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import com.maxmind.geoip2.exception.GeoIp2Exception;

/** Contains a function finding geo information of an IP address using MaxMind's GeoIP2-City. */
public class GeocodeDatabaseReader extends AbstractDatabaseReader {

    public static final String DEFAULT_DATABASE_CITY_PATH = "/usr/share/GeoIP/GeoIP2-City.mmdb";
    public static final String DEFAULT_DATABASE_CITY_PROP = "maxmind.database.city";

    private static final Logger LOG = Logger.getLogger(GeocodeDatabaseReader.class.getName());

    // FIXME: this should be a singleton (small "s") and constructor injected
    private static final IpUtil IP_UTIL = new IpUtil();

    public GeocodeDatabaseReader(String databasePath) throws IOException {
        this.databasePath = databasePath;
        initializeReader();
    }

    public GeocodeDatabaseReader() throws IOException {
        this(null);
    }

    /** Method returning the default CITY database path property name (needed by superclass). */
    public String getDefaultDatabasePathPropertyName() {
        return DEFAULT_DATABASE_CITY_PROP;
    }

    /** Method returning the default CITY database path (needed by superclass). */
    public String getDefaultDatabasePath() {
        return DEFAULT_DATABASE_CITY_PATH;
    }

    /** Mean to provide the superclass access to the logger. */
    protected Logger getLogger() {
        return LOG;
    }

    /**
     * Perform the geocoding.
     *
     * @param ip the IP to geocode
     * @return the Geocode response object
     */
    public RefineryGeocodeDatabaseResponse getResponse(String ip) {
        // Only get geo-code data for non-internal IPs
        if (IP_UTIL.getNetworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET) {
            return UNKNOWN_GEOCODE;
        }

        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            return  RefineryGeocodeDatabaseResponse.from(reader.city(ipAddress));
        } catch (IOException | GeoIp2Exception ex) {
            // Suppress useless messages about 127.0.0.1 not found in database.
            if (!ex.getMessage().contains("The address 127.0.0.1 is not in the database.")) {
                LOG.warn(ex);
            }
            return UNKNOWN_GEOCODE;
        }
    }
}
