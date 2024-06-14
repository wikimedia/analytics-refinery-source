package org.wikimedia.analytics.refinery.core.maxmind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikimedia.analytics.refinery.core.maxmind.RefineryCountryDatabaseResponse.UNKNOWN_COUNTRY;
import static org.wikimedia.analytics.refinery.core.maxmind.RefineryCountryDatabaseResponse.from;

import org.junit.Test;

import com.maxmind.geoip2.model.CountryResponse;

public class TestRefineryCountryDatabaseResponse {

    @Test
    public void responseCreatedWithDefaultValuesWhenPassedNull() {
        assertThat(from(null))
                .isEqualTo(UNKNOWN_COUNTRY);
    }

    @Test
    public void responseCreatedWithDefaultValuesWhenPassedNullProperties() {
        assertThat(from(new CountryResponse(null, null, null, null, null, null)))
            .isEqualTo(UNKNOWN_COUNTRY);
    }

}
