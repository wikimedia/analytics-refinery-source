/**
    * Copyright (C) 2014  Wikimedia Foundation
    * <p/>
    * Licensed under the Apache License, Version 2.0 (the "License");
    * you may not use this file except in compliance with the License.
    * You may obtain a copy of the License at
    * <p/>
    * http://www.apache.org/licenses/LICENSE-2.0
    * <p/>
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */

package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;


/**
 * A Hive UDF to check whether a String is a valid pageview title as defined
 * in [[org.wikimedia.analytics.refinery.core.PageviewDefinition.isValidPageTitle]]
 *
 * <p/>
 * <p/>
 * Hive Usage:
 * ADD JAR /path/to/refinery-hive.jar;
 * CREATE TEMPORARY FUNCTION is_valid_pageview_title AS
 * 'org.wikimedia.analytics.refinery.hive.IsValidPageviewTitleUDF';
 * SELECT
 * is_valid_pageview_title(page_title) as valid_page_title,
 * count(1) as cnt
 * FROM
 * wmf.pageview_hourly
 * WHERE
 * AND year=2020
 * AND month=11
 * AND day=24
 * AND hour=0
 * GROUP BY
 * is_valid_pageview_title(page_title)
 * ORDER BY cnt desc
 * LIMIT 10
 * ;
 */
@Description(name = "is_valid_pageview_title",
    value = "_FUNC_(title) - Returns true if the title is a valid pageview one",
    extended = "")
@UDFType(deterministic = true)
public class IsValidPageviewTitleUDF extends UDF {
    public boolean evaluate(String title) {
        return PageviewDefinition.getInstance().isValidPageTitle(title);
    }
}