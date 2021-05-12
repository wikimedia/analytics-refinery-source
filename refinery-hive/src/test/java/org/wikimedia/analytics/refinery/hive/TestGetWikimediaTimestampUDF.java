/*
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

import junitparams.JUnitParamsRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(JUnitParamsRunner.class)
public class TestGetWikimediaTimestampUDF {

	GetMediawikiTimestampUDF udf = new GetMediawikiTimestampUDF();

	@Test
	public void testNullInput() {
		assertNull(
				"GetMediawikiTimestampUDF returns NULL for NULL input",
				udf.evaluate(null)
		);
	}

	@Test
	public void testIncorrectInput() {
		assertNull(
				"GetMediawikiTimestampUDF returns NULL for incorrectly formatted input",
				udf.evaluate("wrong format")
		);
	}

	@Test
	public void testCorrectSimpleISOInput() {
		assertEquals(
				"GetMediawikiTimestampUDF returns correct value for a ISO-formatted input (no millis nor timezone)",
				"19802810053204",
				udf.evaluate("1980-28-10T05:32:04")
		);
	}

	@Test
	public void testCorrectSimpleSQLInput() {
		assertEquals(
				"GetMediawikiTimestampUDF returns correct value for a SQL-formatted input (no millis nor timezone)",
				"19802810053204",
				udf.evaluate("1980-28-10 05:32:04")
		);
	}

	@Test
	public void testCorrectInputWithMillis() {
		assertEquals(
				"GetMediawikiTimestampUDF returns correct value for a ISO-formatted input with millis (no timezone)",
				"19802810053204",
				udf.evaluate("1980-28-10T05:32:04.128")
		);
	}

	@Test
	public void testCorrectInputWithZTimezone() {
		assertEquals(
				"GetMediawikiTimestampUDF returns correct value for a ISO-formatted input with Z timezone (no millis)",
				"19802810053204",
				udf.evaluate("1980-28-10T05:32:04Z")
		);
	}

	@Test
	public void testCorrectInputWithHourTimezone() {
		assertEquals(
				"GetMediawikiTimestampUDF returns correct value for a SQL-formatted input with hour timezone (no millis)",
				"19802810053204",
				udf.evaluate("1980-28-10 05:32:04+10")
		);
	}

	@Test
	public void testCorrectInputWithFullTimezone() {
		assertEquals(
				"GetMediawikiTimestampUDF returns correct value for a ISO-formatted input with millis and full timezone",
				"19802810053204",
				udf.evaluate("1980-28-10T05:32:04.124+10:00")
		);
	}

}
