package org.wikimedia.analytics.refinery.core.maxmind;

import com.maxmind.geoip2.model.IspResponse;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Objects.firstNonNull;
import static java.util.Collections.unmodifiableMap;

/**
 * MaxMind's GeoIP2-ISP information that we query for given an IP
 */
@Immutable
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

    public static final RefineryISPDatabaseResponse UNKNOWN_ISP_DATABASE_RESPONSE =
            new RefineryISPDatabaseResponse(UNKNOWN_VALUE, UNKNOWN_VALUE, UNKNOWN_VALUE, UNKNOWN_AUTONOMOUS_SYSTEM_NUMBER);

    @Nonnull private final String isp;
    @Nonnull private final String organization;
    @Nonnull private final String autonomousSystemOrg;
    @Nonnull private final int autonomousSystemNumber;

    public RefineryISPDatabaseResponse(String isp, String organization, String autonomousSystemOrg, int autonomousSystemNumber) {
        this.isp = firstNonNull(isp, UNKNOWN_VALUE);
        this.organization = firstNonNull(organization, UNKNOWN_VALUE);
        this.autonomousSystemOrg = firstNonNull(autonomousSystemOrg, UNKNOWN_VALUE);
        this.autonomousSystemNumber = firstNonNull(autonomousSystemNumber, UNKNOWN_AUTONOMOUS_SYSTEM_NUMBER);
    }

    @Nonnull
    public static RefineryISPDatabaseResponse from(@Nullable IspResponse response) {
        if (response == null) return UNKNOWN_ISP_DATABASE_RESPONSE;

        return new RefineryISPDatabaseResponse(
                response.getIsp(),
                response.getOrganization(),
                response.getAutonomousSystemOrganization(),
                response.getAutonomousSystemNumber()
        );
    }

    public String getIsp(){
        return isp;
    }

    public String getOrganization(){
        return organization;
    }

    public String getAutonomousSystemOrg(){
        return autonomousSystemOrg;
    }

    public int getAutonomousSystemNumber(){
        return autonomousSystemNumber;
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

        return unmodifiableMap(defaultISPData);
    }
}
