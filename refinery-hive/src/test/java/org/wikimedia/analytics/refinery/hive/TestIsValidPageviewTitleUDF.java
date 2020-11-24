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

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class TestIsValidPageviewTitleUDF {

    // No state in this UDF, can be reused
    private IsValidPageviewTitleUDF udf = new IsValidPageviewTitleUDF();

    @Test
    public void testValidTitle() {
        assertTrue(
            "Assert valid title is valid",
            udf.evaluate("Some_valid_title")
        );
    }

    @Test
    public void testInvalidTitles() {
        assertFalse(
                "Assert invalid title with EOL in the middle is invalid",
                udf.evaluate("Some\ninvalid_title")
        );

        assertFalse(
                "Assert invalid title with EOL at the end is invalid",
                udf.evaluate("Some_invalid_title\n")
        );
    }
}
