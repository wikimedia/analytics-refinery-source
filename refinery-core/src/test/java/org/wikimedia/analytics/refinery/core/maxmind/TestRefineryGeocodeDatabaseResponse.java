package org.wikimedia.analytics.refinery.core.maxmind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikimedia.analytics.refinery.core.maxmind.RefineryGeocodeDatabaseResponse.UNKNOWN_GEOCODE;
import static org.wikimedia.analytics.refinery.core.maxmind.RefineryGeocodeDatabaseResponse.from;

import org.junit.Test;

import com.maxmind.geoip2.model.CityResponse;

public class TestRefineryGeocodeDatabaseResponse {

    @Test
    public void responseCreatedWithDefaultValuesWhenPassedNull() {
        assertThat(from(null))
                .isEqualTo(UNKNOWN_GEOCODE);
    }

    @Test
    public void responseCreatedWithDefaultValuesWhenPassedNullProperties() {
        assertThat(from(new CityResponse(null, null, null, null, null, null, null, null, null, null)))
            .isEqualTo(UNKNOWN_GEOCODE);
    }

}
