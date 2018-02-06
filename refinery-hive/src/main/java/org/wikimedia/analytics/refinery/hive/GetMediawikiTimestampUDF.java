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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.ISODateTimeFormat;
import org.wikimedia.analytics.refinery.core.Utilities;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A hive UDF to convert an ISO-timestamp (YYYY-MM-DDTHH:MM:SS)
 * or SQL-timestamp (YYYY-MM-DD HH:MM:SS)
 * into a Mediawiki timestamp (YYYYMMDDHHMMSS).
 * Any timezone or millisecond information is lost in the conversion.
 *
 * Return NULL if the given timestamp is null
 * or incorrectly formatted.
 */
public class GetMediawikiTimestampUDF extends UDF {

    public static final Pattern ISO_DT_PATTERN = Pattern.compile(
            "^(\\d{4})-(\\d{2})-(\\d{2})[T| ](\\d{2}):(\\d{2}):(\\d{2})(\\.\\d{3})*(Z|\\+\\d{2}(:\\d{2})?)?$"
    );

    public String evaluate(String dt) {
        if (StringUtils.isNotEmpty(dt)) {
            Matcher m = ISO_DT_PATTERN.matcher(dt);
            if (m.matches()) {
                return m.group(1) + m.group(2) + m.group(3) + m.group(4) + m.group(5) + m.group(6);
            }
        }
        return null;
    }
}