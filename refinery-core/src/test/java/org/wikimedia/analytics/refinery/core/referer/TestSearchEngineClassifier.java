package org.wikimedia.analytics.refinery.core.referer;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Deprecated
@RunWith(JUnitParamsRunner.class)
public class TestSearchEngineClassifier {

    SearchEngineClassifier external_inst;

    @Before
    public void setUp() {
         external_inst = SearchEngineClassifier.getInstance();
    }


    @Test
    @FileParameters(
            value = "src/test/resources/search_engine_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )

    public void testIdentifySearchEngine(
            String test_description,
            String referer,
            String referer_class,
            boolean is_external,
            String search_engine
    ) {

        assertEquals(
                test_description,
                search_engine,
                external_inst.identifySearchEngine(referer)
        );
    }
}
