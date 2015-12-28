package org.wikimedia.analytics.refinery.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;

@RunWith(JUnitParamsRunner.class)
public class TestSearchEngineClassifier {

    @Test
    @FileParameters(
            value = "src/test/resources/referer_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )

    public void testRefererClassify(
            String test_description,
            String referer,
            String referer_class,
            boolean is_external,
            String search_engine
    ) {
        SearchEngineClassifier external_inst = SearchEngineClassifier.getInstance();

        assertEquals(
                test_description,
                referer_class,
                external_inst.refererClassify(referer)
        );
    }

    @Test
    @FileParameters(
            value = "src/test/resources/referer_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )

    public void testIsExternalSearch(
            String test_description,
            String referer,
            String referer_class,
            boolean is_external,
            String search_engine
    ) {
        SearchEngineClassifier external_inst = SearchEngineClassifier.getInstance();

        assertEquals(
                test_description,
                is_external,
                external_inst.isExternalSearch(referer)
        );
    }

    @Test
    @FileParameters(
            value = "src/test/resources/referer_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )

    public void testIdentifySearchEngine(
            String test_description,
            String referer,
            String referer_class,
            boolean is_external,
            String search_engine
    ) {
        SearchEngineClassifier external_inst = SearchEngineClassifier.getInstance();

        assertEquals(
                test_description,
                search_engine,
                external_inst.identifySearchEngine(referer)
        );
    }
}
