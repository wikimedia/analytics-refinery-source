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

package org.wikimedia.analytics.refinery.core.maxmind;

import com.maxmind.geoip2.exception.GeoIp2Exception;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import java.io.IOException;
import java.net.InetAddress;

import static org.wikimedia.analytics.refinery.core.maxmind.RefineryISPDatabaseResponse.UNKNOWN_ISP_DATABASE_RESPONSE;

/**
 * Contains functions to find ISP information of an IP address using MaxMind's GeoIP2-ISP
 */
public class ISPDatabaseReader extends AbstractDatabaseReader {

    public static final String DEFAULT_DATABASE_ISP_PATH = "/usr/share/GeoIP/GeoIP2-ISP.mmdb";
    public static final String DEFAULT_DATABASE_ISP_PROP = "maxmind.database.isp";

    private static final Logger LOG = Logger.getLogger(ISPDatabaseReader.class.getName());

    private final IpUtil ipUtil = new IpUtil();

    public ISPDatabaseReader(String databasePath) throws IOException {
        this.databasePath = databasePath;
        initializeReader();
    }

    public ISPDatabaseReader() throws IOException {
        this(null);
    }

    /**
     * Method returning the default ISP database path property name (needed by superclass)
     * @return the default ISP database path property name
     */
    public String getDefaultDatabasePathPropertyName() {
        return DEFAULT_DATABASE_ISP_PROP;
    }

    /**
     * Method returning the default ISP database path (needed by superclass)
     * @return the default ISP database path
     */
    public String getDefaultDatabasePath() {
        return DEFAULT_DATABASE_ISP_PATH;
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
    public RefineryISPDatabaseResponse getResponse(final String ip) {
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);

            // Only get ISP value for non-internal IPs
            if (ipUtil.getNetworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET)
                return UNKNOWN_ISP_DATABASE_RESPONSE;

            return RefineryISPDatabaseResponse.from(reader.isp(ipAddress));
        } catch (IOException | GeoIp2Exception ex) {
            LOG.warn(ex);
            return UNKNOWN_ISP_DATABASE_RESPONSE;
        }
    }

}
