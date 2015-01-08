/**
 * Copyright (C) 2014 Wikimedia Foundation
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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.*;
import org.apache.log4j.Logger;

/**
 * Contains functions to find geo information of an IP address using Maxmind's GeoIP2
 */
public class Geocode {

    private DatabaseReader countryDatabaseReader;
    private DatabaseReader cityDatabaseReader;

    //Constants to hold the keys to use in geo-coded data map
    private static final String CONTINENT = "continent";
    private static final String COUNTRY_CODE = "country_code";
    private static final String COUNTRY = "country";
    private static final String SUBDIVISION = "subdivision";
    private static final String CITY = "city";
    private static final String POSTAL_CODE = "postal_code";
    private static final String LATITUDE = "latitude";
    private static final String LONGITUDE = "longitude";
    private static final String TIMEZONE = "timezone";

    private static final String UNKNOWN_COUNTRY_CODE = "--";
    private static final String UNKNOWN_VALUE = "Unknown";

    static final Logger LOG = Logger.getLogger(Geocode.class.getName());

    public Geocode() throws IOException {
        // Default paths to Maxmind database
        String defaultCountryDatabasePath = "/usr/share/GeoIP/GeoIP2-Country.mmdb";
        String defaultCityDatabasePath = "/usr/share/GeoIP/GeoIP2-City.mmdb";

        String countryDatabasepath = System.getProperty("maxmind.database.country", defaultCountryDatabasePath);
        String cityDatabasePath = System.getProperty("maxmind.database.city", defaultCityDatabasePath);

        LOG.info("Country Database: " + countryDatabasepath);
        LOG.info("City database: " + cityDatabasePath);

        countryDatabaseReader = new DatabaseReader.Builder(new File(countryDatabasepath)).build();
        cityDatabaseReader = new DatabaseReader.Builder(new File(cityDatabasePath)).build();
    }

    /**
     * Gets the country code for the given IP
     * @param ip
     *      String IP address
     * @return
     *      String
     */
    public final String getCountryCode(final String ip) {
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CountryResponse response = countryDatabaseReader.country(ipAddress);
            Country country = response.getCountry();
            return country.getIsoCode();
        } catch (UnknownHostException hEx) {
            LOG.warn(hEx);
            return UNKNOWN_COUNTRY_CODE;
        } catch (IOException iEx) {
            LOG.warn(iEx);
            return UNKNOWN_COUNTRY_CODE;
        } catch (GeoIp2Exception gEx) {
            LOG.warn(gEx);
            return UNKNOWN_COUNTRY_CODE;
        }
    }

    /**
     * Gets a map with geo-code fields for the given IP
     * @param ip
     *      String Ip address
     * @return
     *      Map
     */
    public final Map<String, Object> getGeocodedData(final String ip) {

        InetAddress ipAddress = null;
        //Initialize map with default values
        Map<String, Object> geoData = getDefaultMap();

        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException hEx) {
            LOG.warn(hEx);
            return geoData;
        }

        CityResponse response = null;
        try {
            response = cityDatabaseReader.city(ipAddress);
        } catch (IOException iEx) {
            LOG.warn(iEx);
            return geoData;
        } catch (GeoIp2Exception gEx) {
            LOG.warn(gEx);
            return geoData;
        }

        if (response == null)
            return geoData;

        Continent continent = response.getContinent();
        if (continent != null && continent.getName() != null)
            geoData.put(CONTINENT, continent.getName());

        Country country = response.getCountry();
        if (country != null && country.getIsoCode() != null) {
            geoData.put(COUNTRY_CODE, country.getIsoCode());
            geoData.put(COUNTRY, country.getName());
        }

        List<Subdivision> subdivisions = response.getSubdivisions();
        if (subdivisions != null && subdivisions.size() > 0) {
            geoData.put(SUBDIVISION, subdivisions.get(0).getName());
        }

        City city = response.getCity();
        if (city != null && city.getName() != null)
            geoData.put(CITY, city.getName());

        Postal postal = response.getPostal();
        if (postal != null && postal.getCode() != null)
            geoData.put(POSTAL_CODE, postal.getCode());

        Location location = response.getLocation();
        if (location != null) {
            geoData.put(LATITUDE, location.getLatitude());
            geoData.put(LONGITUDE, location.getLongitude());
            if (location.getTimeZone() != null)
                geoData.put(TIMEZONE, location.getTimeZone());
        }

        return geoData;
    }

    /**
     * Creates a new geo data map with default values for all fields
     * @return Map
     */
    private Map<String, Object> getDefaultMap() {
        Map<String, Object> defaultGeoData = new HashMap<String, Object>();
        defaultGeoData.put(CONTINENT, UNKNOWN_VALUE);
        defaultGeoData.put(COUNTRY_CODE, UNKNOWN_COUNTRY_CODE);
        defaultGeoData.put(COUNTRY, UNKNOWN_VALUE);
        defaultGeoData.put(SUBDIVISION, UNKNOWN_VALUE);
        defaultGeoData.put(CITY, UNKNOWN_VALUE);
        defaultGeoData.put(POSTAL_CODE, UNKNOWN_VALUE);
        defaultGeoData.put(LATITUDE, -1);
        defaultGeoData.put(LONGITUDE, -1);
        defaultGeoData.put(TIMEZONE, UNKNOWN_VALUE);

        return defaultGeoData;
    }
}
