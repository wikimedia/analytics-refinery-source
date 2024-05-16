package org.wikimedia.analytics.refinery.core.maxmind;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;

import static com.google.common.base.Objects.firstNonNull;

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
        this.countryCode = firstNonNull(countryCode, UNKNOWN_COUNTRY_CODE);
        this.countryName = firstNonNull(countryName, UNKNOWN_VALUE);
    }

    public String getCountryName(){
        return countryName;
    }

    public String getCountryCode(){
        return countryCode;
    }

    @Nonnull
    public static RefineryCountryDatabaseResponse from(@Nullable CountryResponse response) {
        if (response == null) return UNKNOWN_COUNTRY;

        Country country = response.getCountry();
        return new RefineryCountryDatabaseResponse(country.getIsoCode(), country.getName());
    }

}


