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

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.IspResponse;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Contains functions to find ISP information of an IP address using MaxMind's GeoIP2-ISP
 */
public class ISPDatabaseReader {

    static final Logger LOG = Logger.getLogger(ISPDatabaseReader.class.getName());

    private DatabaseReader reader;

    private final IpUtil ipUtil = new IpUtil();

    public ISPDatabaseReader(DatabaseReader reader) throws IOException{
        this.reader = reader;
    }

    /**
     * Given an IP return ISP associated info with sensible defaults
     * @param ip
     * @return
     */
    public RefineryISPDatabaseResponse getResponse(final String ip) {

        InetAddress ipAddress;
        IspResponse maxMindResponse = null;

        RefineryISPDatabaseResponse refineryResponse = new RefineryISPDatabaseResponse();

        // Return empty response for null IP
        if (ip == null) {
            return refineryResponse;
        }

        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException hEx) {
            LOG.warn(hEx);
            return refineryResponse;
        }

        // Only get ISP value for non-internal IPs
        if (ipUtil.getNetworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET) {
            return refineryResponse;
        }

        try {
            maxMindResponse = reader.isp(ipAddress);
        } catch (IOException|GeoIp2Exception ex ) {
            LOG.warn(ex);
        }

        if (maxMindResponse == null) {
            return refineryResponse;
        }
        String isp = maxMindResponse.getIsp();
        if (isp != null) {
            refineryResponse.setIsp(isp);
        }

        String organization = maxMindResponse.getOrganization();
        if (organization != null) {
            refineryResponse.setOrganization(organization);
        }

        String autonomousSystemOrganization = maxMindResponse.getAutonomousSystemOrganization();
        if (autonomousSystemOrganization != null) {
            refineryResponse.setAutonomousSystemOrg(autonomousSystemOrganization);
        }

        Integer autonomousSystemNumber = maxMindResponse.getAutonomousSystemNumber();
        if (autonomousSystemNumber != null) {
            refineryResponse.setAutonomousSystemNumber(autonomousSystemNumber);
        }

        return refineryResponse;
    }


}
