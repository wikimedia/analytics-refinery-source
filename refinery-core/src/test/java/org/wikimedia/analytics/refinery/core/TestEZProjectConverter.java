package org.wikimedia.analytics.refinery.core;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZProjectConverter;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestEZProjectConverter {
    @Test
    @FileParameters(
            value = "src/test/resources/EZ-to-standard-project-names-data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testAllTheProjects(String ez_name, String standard_name) {
        String result = EZProjectConverter.ezProjectToStandard(ez_name);
        assertEquals(result, standard_name);
    }
}