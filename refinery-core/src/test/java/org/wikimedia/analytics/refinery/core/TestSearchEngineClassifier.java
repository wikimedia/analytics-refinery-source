package org.wikimedia.analytics.refinery.core;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestSearchEngineClassifier {

    SearchEngineClassifier external_inst ;

    @Before
    public void setUp(){
         external_inst = SearchEngineClassifier.getInstance();
    }

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

        assertEquals(
                test_description,
                referer_class,
                external_inst.getRefererClass(referer).getRefLabel()
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

        assertEquals(
                test_description,
                search_engine,
                external_inst.identifySearchEngine(referer)
        );
    }
}
