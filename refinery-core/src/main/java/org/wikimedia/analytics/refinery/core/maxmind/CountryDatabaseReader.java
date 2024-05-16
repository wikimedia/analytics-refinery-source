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

import com.maxmind.geoip2.exception.GeoIp2Exception;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import java.io.IOException;
import java.net.InetAddress;

/*
Contains functions to find country information of an IP address using MaxMind's GeoIP2-Country
*/
public class CountryDatabaseReader extends AbstractDatabaseReader {

    public static final String DEFAULT_DATABASE_COUNTRY_PATH = "/usr/share/GeoIP/GeoIP2-Country.mmdb";
    public static final String DEFAULT_DATABASE_COUNTRY_PROP = "maxmind.database.country";

    private final IpUtil ipUtil = new IpUtil();

    private static final Logger LOG = Logger.getLogger(CountryDatabaseReader.class.getName());

    public CountryDatabaseReader(String databasePath) throws IOException {
        this.databasePath = databasePath;
        initializeReader();
    }

    public CountryDatabaseReader() throws IOException {
        this(null);
    }

    /**
     * Method returning the default COUNTRY database path property name (needed by superclass)
     * @return the default COUNTRY database path property name
     */
    public String getDefaultDatabasePathPropertyName() {
        return DEFAULT_DATABASE_COUNTRY_PROP;
    }

    /**
      * Method returning the default COUNTRY database path (needed by superclass)
      */
    public String getDefaultDatabasePath() {
        return DEFAULT_DATABASE_COUNTRY_PATH;
    }

    /**
      * Mean to provide the superclass access to the logger
      */
    protected Logger getLogger() {
        return LOG;
    }

     /**
      * Perform the geocoding.
      *
      * @param ip the IP to geocode
      */
    public RefineryCountryDatabaseResponse getResponse(String ip) {
        // Only get country for non-internal IPs
        if (ipUtil.getNetworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET) {
           return RefineryCountryDatabaseResponse.UNKNOWN_COUNTRY;
        }

        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            return RefineryCountryDatabaseResponse.from(reader.country(ipAddress));
        }  catch (IOException |GeoIp2Exception ex) {
            LOG.warn(ex);
            return RefineryCountryDatabaseResponse.UNKNOWN_COUNTRY;
        }
    }
}
