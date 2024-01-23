package org.wikimedia.analytics.refinery.core;

import java.util.Locale;

public class LocaleUtil {

    public static final String UNKNOWN_VALUE = "Unknown";

    /**
     * Translate a country code into the country name
     *
     * @param countryCode the country-code for which to get the country name
     *
     * @return String country name
     */
    public static String getCountryName(String countryCode) {
        if (countryCode == null){
            countryCode = "";
        }
        Locale l = new Locale("", countryCode);
        String displayCountry = l.getDisplayCountry(Locale.ROOT);
        return displayCountry.equalsIgnoreCase(countryCode) ? UNKNOWN_VALUE : displayCountry;
    }

}
