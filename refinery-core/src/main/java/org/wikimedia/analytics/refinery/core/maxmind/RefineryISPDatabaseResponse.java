package org.wikimedia.analytics.refinery.core.maxmind;

import java.util.HashMap;
import java.util.Map;

/**
 * MaxMind's GeoIP2-ISP information that we query for given an IP
 */
public class RefineryISPDatabaseResponse {
    // Constants to hold the keys to use in data map
    public static final String ISP = "isp";
    public static final String ORGANISATION = "organization";
    public static final String AUTONOMOUS_SYSTEM_ORGANIZATION = "autonomous_system_organization";
    public static final String AUTONOMOUS_SYSTEM_NUMBER = "autonomous_system_number";

    // Expected range is 0 to 4,294,967,295
    // see https://en.wikipedia.org/wiki/Autonomous_system_(Internet)
    private static final int UNKNOWN_AUTONOMOUS_SYSTEM_NUMBER = -1;
    private static final String UNKNOWN_VALUE = "Unknown";

    private String isp = UNKNOWN_VALUE;

    private String organization = UNKNOWN_VALUE;

    private String autonomousSystemOrg = UNKNOWN_VALUE;

    private int autonomousSystemNumber = UNKNOWN_AUTONOMOUS_SYSTEM_NUMBER;

    public RefineryISPDatabaseResponse(){

    }

    public String getIsp(){
        return isp;
    }

    public void setIsp(String isp){
        this.isp = isp;
    }

    public String getOrganization(){
        return organization;
    }

    public void setOrganization(String organization){
        this.organization = organization;
    }

    public String getAutonomousSystemOrg(){
        return autonomousSystemOrg;
    }

    public void setAutonomousSystemOrg(String autonomousSystemOrg){
        this.autonomousSystemOrg = autonomousSystemOrg;
    }

    public int getAutonomousSystemNumber(){
        return autonomousSystemNumber;
    }

    public void setAutonomousSystemNumber(int autonomousSystemNumber){
        this.autonomousSystemNumber = autonomousSystemNumber;
    }

    /**
     * Creates a new ISP map with default values for all fields
     * @return Map the map of default ISP information (unknown)
     */
    public Map<String, String> getMap() {
        Map<String, String> defaultISPData = new HashMap<>();
        defaultISPData.put(ISP, this.isp);
        defaultISPData.put(ORGANISATION, this.organization);
        defaultISPData.put(AUTONOMOUS_SYSTEM_ORGANIZATION, this.autonomousSystemOrg);
        defaultISPData.put(AUTONOMOUS_SYSTEM_NUMBER, Integer.toString(this.autonomousSystemNumber));

        return defaultISPData;
    }
}
