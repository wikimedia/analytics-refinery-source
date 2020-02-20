package org.wikimedia.analytics.refinery.core.maxmind;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.IpUtil;

import java.io.IOException;
import java.net.InetAddress;

/*
Contains functions to find country information of an IP address using MaxMind's GeoIP2-Country
*/
public class CountryDatabaseReader  {

    protected DatabaseReader reader;

    private final IpUtil ipUtil = new IpUtil();

    static final Logger LOG = Logger.getLogger(CountryDatabaseReader.class.getName());

    public CountryDatabaseReader(DatabaseReader reader) throws IOException{
              this.reader = reader;
    }

    public RefineryCountryDatabaseResponse getResponse(String ip) {
        CountryResponse maxMindResponse;

        RefineryCountryDatabaseResponse refineryResponse = new RefineryCountryDatabaseResponse();

        // Return empty response for null IP
        if (ip == null) {
            return refineryResponse;
        }

        // Only get country for non-internal IPs
        if (ipUtil.getNetworkOrigin(ip) != IpUtil.NetworkOrigin.INTERNET) {
           return refineryResponse;
        }
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            maxMindResponse = reader.country(ipAddress);
            Country country = maxMindResponse.getCountry();

            String isoCode = country.getIsoCode();

            if ( isoCode != null) {
                refineryResponse.setCountryCode(isoCode);
            }

            String name = country.getName();
            if ( name != null) {
                refineryResponse.setCountryName(name);
            }

        }  catch (IOException |GeoIp2Exception ex) {
            LOG.warn(ex);
        }

        return refineryResponse;
    }
}
