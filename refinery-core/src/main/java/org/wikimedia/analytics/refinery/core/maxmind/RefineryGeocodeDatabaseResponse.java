package org.wikimedia.analytics.refinery.core.maxmind;

import com.google.common.base.Objects;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.*;

import static com.google.common.base.Objects.firstNonNull;
import static java.util.Collections.unmodifiableMap;

/**
 * MaxMind's GeoIP2-City information that we query for given an IP
 */
@Immutable
public class RefineryGeocodeDatabaseResponse {

    // Constants to hold the keys to use in data map
    public static final String CONTINENT = "continent";
    public static final String COUNTRY_CODE = "country_code";
    public static final String COUNTRY = "country";
    public static final String SUBDIVISION = "subdivision";
    public static final String SUBDIVISION_CODE = "subdivision_code";
    public static final String CITY = "city";
    public static final String TIMEZONE = "timezone";


    private static final String UNKNOWN_VALUE = "Unknown";

    private static final String UNKNOWN_COUNTRY_OR_SUBDIVISION_CODE = "--";

    public static final RefineryGeocodeDatabaseResponse UNKNOWN_GEOCODE = new RefineryGeocodeDatabaseResponse(
            UNKNOWN_VALUE, UNKNOWN_COUNTRY_OR_SUBDIVISION_CODE, UNKNOWN_VALUE, UNKNOWN_VALUE,
            UNKNOWN_COUNTRY_OR_SUBDIVISION_CODE, UNKNOWN_VALUE, UNKNOWN_VALUE);

    @Nonnull private final String continent;
    @Nonnull private final String countryISOCode;
    @Nonnull private final String country;
    @Nonnull private final String subdivision;
    @Nonnull private final String subdivisionISOCode;
    @Nonnull private final String city;
    @Nonnull private final String timezone;

    public RefineryGeocodeDatabaseResponse(
            @Nullable String continent,
            @Nullable String countryISOCode,
            @Nullable String country,
            @Nullable String subdivision,
            @Nullable String subdivisionISOCode,
            @Nullable String city,
            @Nullable String timezone) {
        this.continent = firstNonNull(continent, UNKNOWN_VALUE);
        this.countryISOCode = firstNonNull(countryISOCode, UNKNOWN_COUNTRY_OR_SUBDIVISION_CODE);
        this.country = firstNonNull(country, UNKNOWN_VALUE);
        this.subdivision = firstNonNull(subdivision, UNKNOWN_VALUE);
        this.subdivisionISOCode = firstNonNull(subdivisionISOCode, UNKNOWN_COUNTRY_OR_SUBDIVISION_CODE);
        this.city = firstNonNull(city, UNKNOWN_VALUE);
        this.timezone = firstNonNull(timezone, UNKNOWN_VALUE);
    }

    public String getContinent(){
        return continent;
    }

    public String getIsoCode(){
        return countryISOCode;
    }

    public String getCountry(){
        return country;
    }

    public String getSubdivision(){
        return subdivision;
    }

    public String getSubdivisionISOCode(){
        return subdivisionISOCode;
    }

    public String getCity(){
        return city;
    }

    public String getTimezone(){
        return timezone;
    }

    /**
     * Converts POJO to a Map
     * @return  Map
     */
    public Map<String, String> getMap(){
        Map<String, String> defaultGeoData = new HashMap<>();
        defaultGeoData.put(CONTINENT, this.continent);
        defaultGeoData.put(COUNTRY_CODE, this.countryISOCode);
        defaultGeoData.put(COUNTRY, this.country);
        defaultGeoData.put(SUBDIVISION, this.subdivision);
        defaultGeoData.put(SUBDIVISION_CODE, this.subdivisionISOCode);
        defaultGeoData.put(CITY, this.city);
        defaultGeoData.put(TIMEZONE, this.timezone);

        return unmodifiableMap(defaultGeoData);
    }

    @Nonnull
    public static RefineryGeocodeDatabaseResponse from(@Nullable CityResponse response) {
        if (response == null) return UNKNOWN_GEOCODE;

        // direct properties of CityResponse are known to be non null, no need to check for nullity
        Iterator<Subdivision> iterator = response.getSubdivisions().iterator();
        Subdivision subdivision = iterator.hasNext() ? iterator.next() : null;

        return new RefineryGeocodeDatabaseResponse(
                response.getContinent().getName(),
                response.getCountry().getIsoCode(),
                response.getCountry().getName(),
                (subdivision != null) ? subdivision.getName() : null,
                (subdivision != null) ? subdivision.getIsoCode() : null,
                response.getCity().getName(),
                response.getLocation().getTimeZone()
        );
    }
}
