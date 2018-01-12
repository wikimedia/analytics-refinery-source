package org.wikimedia.analytics.refinery.core.maxmind;

import java.util.HashMap;
import java.util.Map;

/**
 * MaxMind's GeoIP2-City information that we query for given an IP
 */
public class RefineryGeocodeDatabaseResponse {

    private static final String UNKNOWN_VALUE = "Unknown";

    private static final Double UNKNOWN_LAT_LONG = -1.0;

    private static final String UNKNOWN_COUNTRY_CODE = "--";

    protected String continent;

    protected String countryISOCode;

    protected String country;

    protected String subdivision;

    protected String city;

    protected String postalCode;

    protected Double latitude;

    protected Double longitude;

    protected String timezone;

    // Constants to hold the keys to use in data map
    public static final String CONTINENT = "continent";
    public static final String COUNTRY_CODE = "country_code";
    public static final String COUNTRY = "country";
    public static final String SUBDIVISION = "subdivision";
    public static final String CITY = "city";
    public static final String POSTAL_CODE = "postal_code";
    public static final String LATITUDE = "latitude";
    public static final String LONGITUDE = "longitude";
    public static final String TIMEZONE = "timezone";


    public RefineryGeocodeDatabaseResponse(){
        // default construction just uses "unknowns" for values;
        this.continent = UNKNOWN_VALUE;
        this.countryISOCode = UNKNOWN_COUNTRY_CODE;
        this.country = UNKNOWN_VALUE;
        this.subdivision = UNKNOWN_VALUE;
        this.city = UNKNOWN_VALUE;
        this.postalCode = UNKNOWN_VALUE;
        this.latitude = UNKNOWN_LAT_LONG;
        this.longitude = UNKNOWN_LAT_LONG;
        this.timezone = UNKNOWN_VALUE;

    }

    public String getContinent(){
        return continent;
    }

    public void setContinent(String continent){
        this.continent = continent;
    }

    public String getIsoCode(){
        return countryISOCode;
    }

    public void setIsoCode(String isoCode){
        this.countryISOCode = isoCode;
    }

    public String getCountry(){
        return country;
    }

    public void setCountry(String country){
        this.country = country;
    }

    public String getSubdivision(){
        return subdivision;
    }

    public void setSubdivision(String subdivision){
        this.subdivision = subdivision;
    }

    public String getCity(){
        return city;
    }

    public void setCity(String city){
        this.city = city;
    }

    public String getPostalCode(){
        return postalCode;
    }

    public void setPostalCode(String postalCode){
        this.postalCode = postalCode;
    }

    public Double getLatitude(){
        return latitude;
    }

    public void setLatitude(Double latitute){
        this.latitude = latitute;
    }

    public Double getLongitude(){
        return longitude;
    }

    public void setLongitude(Double longitude){
        this.longitude = longitude;
    }

    public String getTimezone(){
        return timezone;
    }

    public void setTimezone(String timezone){
        this.timezone = timezone;
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
        defaultGeoData.put(CITY, this.city);
        defaultGeoData.put(POSTAL_CODE, this.postalCode);
        defaultGeoData.put(LATITUDE,  this.latitude.toString());
        defaultGeoData.put(LONGITUDE, this.longitude.toString());
        defaultGeoData.put(TIMEZONE, this.timezone);

        return defaultGeoData;
    }
}
