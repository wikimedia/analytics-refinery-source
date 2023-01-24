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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;
import org.wikimedia.analytics.refinery.core.Webrequest;

import java.util.HashMap;
import java.util.Map;


/**
 * A Hive UDF to identify pageview data in a map
 * Map fields are project, language_variant, article.
 * NOTE: this udf only works well if
 * (uri_host, uri_path, uri_query) comes from
 * a webrequest having is_pageview = true
 *
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_pageview_info AS
 *     'org.wikimedia.analytics.refinery.hive.PageviewInfoUDF';
 *   SELECT
 *     get_pageview_info(uri_host, uri_path, uri_query) as pageview_data,
 *     count(*) as cnt
 *   FROM
 *     wmf_raw.webrequest
 *   WHERE
 *    webrequest_source = 'text'
 *     AND year=2014
 *     AND month=12
 *     AND day=7
 *     AND hour=12
 *     AND is_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent)
 *   GROUP BY
 *     get_pageview_info(uri_host, uri_path, uri_query)
 *   ORDER BY cnt desc
 *   LIMIT 10
 *   ;
 */
@UDFType(deterministic = true)
@Description(name = "get_pageview_info",
        value = "_FUNC_(uri_host, uri_path, uri_query) - Returns the pageview information map "
                + "(project, language_variant, article) for the pageview request.",
        extended = "")
public class GetPageviewInfoUDF extends GenericUDF {
    private Converter[] converters = new Converter[3];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[3];

    public static final String PROJECT_KEY = "project";
    public static final String LANGUAGE_VARIANT_KEY = "language_variant";
    public static final String PAGE_TITLE_KEY = "page_title";

    Map<String, String> result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        checkArgsSize(arguments, 3, 3);

        // ... and the parameters have to be strings
        for (int i = 0; i < 3; i++) {
            checkArgPrimitive(arguments, i);
            checkArgGroups(arguments, i, inputTypes, STRING_GROUP);
            obtainStringConverter(arguments, i, inputTypes, converters);
        }

        result = new HashMap<>(3);

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert result != null : "Result object has not yet been initialized, "
                + "but evaluate called";
        // result map has been initialized.

        result.clear();

        String uriHost = getStringValue(arguments, 0, converters);
        String uriPath = getStringValue(arguments, 1, converters);
        String uriQuery = getStringValue(arguments, 2, converters);

        result.put(PROJECT_KEY, Webrequest.getProjectFromHost(uriHost));
        result.put(LANGUAGE_VARIANT_KEY, PageviewDefinition.getLanguageVariantFromPath(uriPath));
        result.put(PAGE_TITLE_KEY, PageviewDefinition.getPageTitleFromUri(uriPath, uriQuery));

        return result;

    }

    @Override
    public String getDisplayString(String[] arguments) {
        String argument;
        if (arguments == null) {
            argument = "<arguments == null>";
        } else if (arguments.length == 3) {
            argument = arguments[0] + ", "
                       + arguments[1] + ", "
                       + arguments[2];
        } else {
            argument = "<arguments of length " + arguments.length + ">";
        }
        return "get_pageview_info(" + argument +")";
    }
}
