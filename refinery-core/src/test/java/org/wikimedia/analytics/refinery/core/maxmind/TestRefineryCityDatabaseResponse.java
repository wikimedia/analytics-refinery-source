package org.wikimedia.analytics.refinery.core.maxmind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikimedia.analytics.refinery.core.maxmind.RefineryISPDatabaseResponse.UNKNOWN_ISP_DATABASE_RESPONSE;
import static org.wikimedia.analytics.refinery.core.maxmind.RefineryISPDatabaseResponse.from;

import org.junit.Test;

import com.maxmind.geoip2.model.IspResponse;

public class TestRefineryCityDatabaseResponse {

    @Test
    public void responseCreatedWithDefaultValuesWhenPassedNull() {
        assertThat(from(null))
                .isEqualTo(UNKNOWN_ISP_DATABASE_RESPONSE);
    }

    @Test
    public void responseCreatedWithDefaultValuesWhenPassedNullProperties() {
        assertThat(from(new IspResponse(null, null, null, null, null)))
            .isEqualTo(UNKNOWN_ISP_DATABASE_RESPONSE);
    }

}
