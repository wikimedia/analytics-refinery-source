/**
 * Copyright (C) 2014  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.refinery.hive;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestIsWikimediaBotUDF {

    @Test
    @FileParameters(
        value = "../refinery-core/src/test/resources/isSpider_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testIsWikimediaBot(
        String test_description,
        boolean isSpider,
        boolean isWikimediaBot,
        String user_agent
    ) {
        IsWikimediaBotUDF udf = new IsWikimediaBotUDF();

        assertEquals(
            test_description,
            isWikimediaBot,
            udf.evaluate(
                user_agent
            )
        );
    }
}