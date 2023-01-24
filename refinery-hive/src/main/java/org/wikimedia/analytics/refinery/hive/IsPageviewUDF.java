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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

/**
 * A Hive UDF to classify a Wikimedia webrequest as a 'pageview'.
 * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
 * for information on how to classify a pageview.
 *
 * Note the last argument(x_analytics) is optional
 *
 * <p/>
 * <p/>
 * Hive Usage:
 * ADD JAR /path/to/refinery-hive.jar;
 * CREATE TEMPORARY FUNCTION is_pageview AS
 * 'org.wikimedia.analytics.refinery.hive.IsPageviewUDF';
 * SELECT
 * LOWER(uri_host) as uri_host,
 * count(*) as cnt
 * FROM
 * wmf_raw.webrequest
 * WHERE
 * webrequest_source = 'text'
 * AND year=2014
 * AND month=12
 * AND day=7
 * AND hour=12
 * AND is_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, [x_analytics_header])
 * GROUP BY
 * LOWER(uri_host)
 * ORDER BY cnt desc
 * LIMIT 10
 * ;
 */
@Description(name = "is_pageview",
        value = "_FUNC_(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) " +
            "- Returns true if the request is a pageview",
        extended = "")
@UDFType(deterministic = true)
public class IsPageviewUDF extends GenericUDF {
    protected boolean checkForXAnalytics = false;
    protected int maxArguments = 7;
    protected int minArguments = 6;

    protected Converter[] converters = new Converter[maxArguments];
    protected PrimitiveCategory[] inputTypes = new PrimitiveCategory[maxArguments];

    /**
     * The pageviewDefinition is marked as transient for Kryo. Kryo will serialize UDF instances without the need for
     * `implements Serializable`. But we don't want to serialize the singleton pageviewDefinition and its caches.
     * We initialize the variable from the singleton in the initialize method.
     */
    protected transient PageviewDefinition pageviewDefinition;

    /**
     * Each spark task has its own UDF object instance, and the initialize method is called for each of those instances
     * at the beginning of the task before processing the data chunk.
     *
     * Checks arguments size. Accepts variable number of arguments, last argument being the raw string that represents
     * the xAnalytics map.
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        //at least we should have 6 arguments
        checkArgsSize(arguments, minArguments, maxArguments);

        if (arguments.length > minArguments){
            checkForXAnalytics = true;
        }

        for (int i = 0; i < arguments.length; i++) {
            checkArgPrimitive(arguments, i);
            checkArgGroups(arguments, i, inputTypes, STRING_GROUP);
            obtainStringConverter(arguments, i, inputTypes, converters);
        }

        // PageviewDefinition.getInstance() is thread safe and will return the uniq instance of JVM.
        pageviewDefinition = PageviewDefinition.getInstance();

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException{
        return pageviewDefinition.isPageview(buildWebrequestData(arguments));
    }

    protected WebrequestData buildWebrequestData(DeferredObject[] arguments) throws HiveException {
        String uriHost = getStringValue(arguments, 0, converters);
        String uriPath = getStringValue(arguments, 1, converters);
        String uriQuery = getStringValue(arguments, 2, converters);
        String httpStatus = getStringValue(arguments, 3, converters);
        String contentType = getStringValue(arguments, 4, converters);
        String userAgent = getStringValue(arguments, 5, converters);

        String rawXAnalyticsHeader = "";

        if (checkForXAnalytics) {
            rawXAnalyticsHeader = getStringValue(arguments, 6, converters);
        }

        return new WebrequestData(uriHost, uriPath, uriQuery, httpStatus, contentType, userAgent, rawXAnalyticsHeader);
    }

    @Override
    public String getDisplayString(String[] arguments) {
        return "isPageView(" + arguments.toString() + ")";
    }
}
