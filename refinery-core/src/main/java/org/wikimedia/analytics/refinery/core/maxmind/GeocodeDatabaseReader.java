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

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Contains a function finding geo information of an IP address using MaxMind's GeoIP2-City
 */
public class GeocodeDatabaseReader {

    static final Logger LOG = Logger.getLogger(GeocodeDatabaseReader.class.getName());

    private DatabaseReader reader;

    private final IpUtil ipUtil = new IpUtil();

    public GeocodeDatabaseReader(DatabaseReader reader) throws IOException{
        this.reader = reader;
    }

    /**
     * Given an IP return geocoded associated info with sensible defaults
     * @param ip
     * @return
     */
    public RefineryGeocodeDatabaseResponse getResponse(final String ip) {

        InetAddress ipAddress;

        CityResponse cityResponse = null;

        RefineryGeocodeDatabaseResponse response = new RefineryGeocodeDatabaseResponse();

        // Only get geo-code data for non-internal IPs
        if (ipUtil.getNeworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET) {
                 return response;
        }

        try {
            ipAddress = InetAddress.getByName(ip);
            cityResponse = reader.city(ipAddress);

        } catch (UnknownHostException ex) {
            LOG.warn(ex);
        } catch (IOException |GeoIp2Exception ex ) {
            LOG.warn(ex);
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

        Postal postal = cityResponse.getPostal();
        if (postal != null) {
            String code = postal.getCode();
            if (code != null) {
                response.setPostalCode(code);
            }
        }

        Location location = cityResponse.getLocation();
        if (location != null) {
            Double lat = location.getLatitude();
            Double lon = location.getLongitude();
            if (lat != null && lon != null) {
                response.setLatitude(lat);
                response.setLongitude(lon);
            }
            if (location.getTimeZone() != null)
                response.setTimezone( location.getTimeZone());
        }

        return response;

    }


}
