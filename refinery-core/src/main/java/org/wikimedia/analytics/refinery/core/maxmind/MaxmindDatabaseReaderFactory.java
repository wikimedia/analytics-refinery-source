package org.wikimedia.analytics.refinery.core.maxmind;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Creates a DatabaseReader for 1 of the three MaxMind databases we use
 */
public class MaxmindDatabaseReaderFactory {

    // Default path to MaxMind Country database
    public static final String DEFAULT_DATABASE_COUNTRY_PATH  = "/usr/share/GeoIP/GeoIP2-Country.mmdb";

    public static final String DEFAULT_DATABASE_CITY_PATH = "/usr/share/GeoIP/GeoIP2-City.mmdb";

    public static final String DEFAULT_DATABASE_ISP_PATH     = "/usr/share/GeoIP/GeoIP2-ISP.mmdb";

    static final Logger LOG = Logger.getLogger(MaxmindDatabaseReaderFactory.class.getName());

    private static  final MaxmindDatabaseReaderFactory instance = new MaxmindDatabaseReaderFactory();

    private MaxmindDatabaseReaderFactory() {

    }

    public static MaxmindDatabaseReaderFactory getInstance() {
         return instance ;
    }

    /**
     * Constructs a DatabaseReader object with the provided MaxMind  database path.
     * This path is 'optional', in that you may set it to null.  If null, the system
     * properties 'some.param.to.db' will be checked for paths.
     * If it is not set will default to propertyDefault
     *
     * @param databasePath
     * @param propertyName
     * @param propertyDefault
     * @return
     * @throws IOException
     */
    public DatabaseReader getReader(String databasePath, String propertyName, String propertyDefault) throws IOException{


        if (databasePath == null || databasePath.isEmpty()) {
            databasePath = System.getProperty(propertyName, propertyDefault);

        }
        LOG.info("Geocode using MaxMind database: " + databasePath);

        return  new DatabaseReader.Builder(new File(databasePath)).build();

    }

    public CountryDatabaseReader getCountryDatabaseReader(String databasePath) throws IOException{

        return new CountryDatabaseReader(this.getReader(databasePath,"maxmind.database.country",
            DEFAULT_DATABASE_COUNTRY_PATH ));

    }

    public CountryDatabaseReader getCountryDatabaseReader() throws IOException{

        return new CountryDatabaseReader(this.getReader("","maxmind.database.country",
            DEFAULT_DATABASE_COUNTRY_PATH ));

    }

    public GeocodeDatabaseReader getGeocodeDatabaseReader(String databasePath) throws IOException {

        return new GeocodeDatabaseReader(this.getReader(databasePath, "maxmind.database.city",
            DEFAULT_DATABASE_CITY_PATH));
    }

    public GeocodeDatabaseReader getGeocodeDatabaseReader() throws IOException {

        return new GeocodeDatabaseReader(this.getReader("", "maxmind.database.city",
            DEFAULT_DATABASE_CITY_PATH));
    }

    public ISPDatabaseReader getISPDatabaseReader(String databasePath) throws IOException {

        return new ISPDatabaseReader(this.getReader(databasePath, "maxmind.database.isp", DEFAULT_DATABASE_ISP_PATH));
    }

    public ISPDatabaseReader getISPDatabaseReader() throws IOException {

        return new ISPDatabaseReader(this.getReader("", "maxmind.database.isp", DEFAULT_DATABASE_ISP_PATH));
    }

}
