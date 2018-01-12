package org.wikimedia.analytics.refinery.core.maxmind;
/**
 * MaxMind's GeoIP2-Country information that we query for given an IP
 */

public class RefineryCountryDatabaseResponse {

    private static final String UNKNOWN_VALUE = "Unknown";

    private static final String UNKNOWN_COUNTRY_CODE = "--";

    protected String countryCode;

    protected String countryName;

    public RefineryCountryDatabaseResponse(){
        // default construction just uses "unknowns" for values;
        this.countryCode = UNKNOWN_COUNTRY_CODE ;
        this.countryName = UNKNOWN_VALUE;
    }

    public String getCountryName(){
        return countryName;
    }

    public void setCountryName(String countryName){
        this.countryName = countryName;
    }

    public String getCountryCode(){
        return countryCode;
    }

    public void setCountryCode(String countryISOCode){
        this.countryCode = countryISOCode;
    }

}


