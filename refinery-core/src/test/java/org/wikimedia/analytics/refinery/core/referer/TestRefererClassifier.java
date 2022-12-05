// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.core.referer;


import junit.framework.TestCase;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestRefererClassifier extends TestCase {

    RefererClassifier external_inst;

    @Before
    public void setUp() {
        external_inst = RefererClassifier.getInstance();
    }

    @Test
    @FileParameters(
            value = "src/test/resources/referer_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )

    //test for classifyReferer method to verify the referer_class
    public void TestGetRefererClass(
            String test_description,
            String referer,
            String referer_class,
            boolean is_external_search,
            String referer_name
    ) {
        assertEquals(
                test_description,
                referer_class,
                external_inst.classifyReferer(referer).getRefererClassLabel().getRefLabel()
                );
    }

    @Test
    @FileParameters(
            value = "src/test/resources/referer_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    //this test will test nameMediaSite and nameSearchEngine methods as well.
    public void testIdentifyReferer(
            String test_description,
            String referer,
            String referer_class,
            boolean is_external_search,
            String referer_name
    ) {

        assertEquals(
                test_description,
                referer_name,
                external_inst.classifyReferer(referer).getRefererIdentified()
        );
    }
}
