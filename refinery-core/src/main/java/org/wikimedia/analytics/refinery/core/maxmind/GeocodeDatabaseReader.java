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
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

/**
 * Contains a function finding geo information of an IP address using MaxMind's GeoIP2-City
 */
public class GeocodeDatabaseReader extends AbstractDatabaseReader {

    public static final String DEFAULT_DATABASE_CITY_PATH = "/usr/share/GeoIP/GeoIP2-City.mmdb";
    public static final String DEFAULT_DATABASE_CITY_PROP = "maxmind.database.city";

    private static final Logger LOG = Logger.getLogger(GeocodeDatabaseReader.class.getName());

    private final static IpUtil ipUtil = new IpUtil();

    public GeocodeDatabaseReader(String databasePath) throws IOException {
        this.databasePath = databasePath;
        initializeReader();
    }

    public GeocodeDatabaseReader() throws IOException {
        this(null);
    }

    /**
     * Method returning the default CITY database path property name (needed by superclass)
     * @return the default CITY database path property name
     */
    public String getDefaultDatabasePathPropertyName() {
        return DEFAULT_DATABASE_CITY_PROP;
    }

    /**
     * Method returning the default CITY database path (needed by superclass)
     * @return the default CITY database path
     */
    public String getDefaultDatabasePath() {
        return DEFAULT_DATABASE_CITY_PATH;
    }

    /**
     * Mean to provide the superclass access to the logger
     */
    protected Logger getLogger() {
        return LOG;
    }

    /**
     * Perform the geocoding
     * @param ip the IP to geocode
     * @return the Geocode response object
     */
    public RefineryGeocodeDatabaseResponse getResponse(final String ip) {

        InetAddress ipAddress;

        CityResponse cityResponse = null;

        RefineryGeocodeDatabaseResponse response = new RefineryGeocodeDatabaseResponse();

        // Only get geo-code data for non-internal IPs
        if (ipUtil.getNetworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET) {
                 return response;
        }

        try {
            ipAddress = InetAddress.getByName(ip);
            cityResponse = reader.city(ipAddress);

        } catch (IOException | GeoIp2Exception ex) {
            // Suppress useless messages about 127.0.0.1 not found in database.
            if (!ex.getMessage().contains("The address 127.0.0.1 is not in the database.")) {
                LOG.warn(ex);
            }
        }

        if (cityResponse == null)     {
            return response;

        }
        Continent continent = cityResponse.getContinent();

        if (continent != null) {
            String name = continent.getName();
            if (name != null) {
                response.setContinent(name);
            }
        }

        Country country = cityResponse.getCountry();
        if (country != null) {
            String name = country.getName();
            String isoCode = country.getIsoCode();
            if (name != null && isoCode != null) {
                response.setCountry(name);
                response.setIsoCode(isoCode);
            }
        }

        List<Subdivision> subdivisions = cityResponse.getSubdivisions();
        if (subdivisions != null && subdivisions.size() > 0) {
            Subdivision subdivision = subdivisions.get(0);
            if (subdivision != null) {
                String name = subdivision.getName();
                if (name != null) {
                    response.setSubdivision(name);
                }
            }
        }

        City city = cityResponse.getCity();
        if (city != null) {
            String name = city.getName();
            if (name != null) {
                response.setCity(name);
            }
        }

        Location location = cityResponse.getLocation();
        if (location != null) {
            String timezone = location.getTimeZone();
            if (timezone != null) {
                response.setTimezone(timezone);
            }
        }

        return response;

    }


}
