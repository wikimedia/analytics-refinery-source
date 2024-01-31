package org.wikimedia.analytics.refinery.core.maxmind;

import javax.annotation.concurrent.Immutable;

/**
 * MaxMind's GeoIP2-Country information that we query for given an IP
 */

@Immutable
public class RefineryCountryDatabaseResponse {

    public static final String UNKNOWN_VALUE = "Unknown";

    public static final String UNKNOWN_COUNTRY_CODE = "--";

    public static final RefineryCountryDatabaseResponse UNKNOWN_COUNTRY = new RefineryCountryDatabaseResponse(UNKNOWN_COUNTRY_CODE, UNKNOWN_VALUE);

    private final String countryCode;

    private final String countryName;

    public RefineryCountryDatabaseResponse(String countryCode, String countryName) {
        if (countryCode == null) this.countryCode = UNKNOWN_COUNTRY_CODE;
        else this.countryCode = countryCode;

        if (countryName == null) this.countryName = UNKNOWN_VALUE;
        else this.countryName = countryName;
    }

    public String getCountryName(){
        return countryName;
    }

    public String getCountryCode(){
        return countryCode;
    }

}


