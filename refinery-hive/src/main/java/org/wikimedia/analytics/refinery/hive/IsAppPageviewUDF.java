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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;

/**
 * A Hive UDF to classify a Wikimedia webrequest as an app pageview.
 * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
 *      and isPageviewUDF for information on how to classify a pageview generally.
 *
 * This is not a /complete/ definition - it was initially a private method. As a
 * result it does not do, for example, HTTP status filtering. See the example
 * query below for how to solve for that.
 *
 * Note the last argument(x_analytics) is optional

 *
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION is_app_pageview AS
 *     'org.wikimedia.analytics.refinery.hive.IsAppPageviewUDF';
 *   SELECT
 *     LOWER(uri_host) as uri_host,
 *     count(*) as cnt
 *   FROM
 *     wmf_raw.webrequest
 *   WHERE year=2014
 *     AND month=12
 *     AND day=7
 *     AND hour=12
 *     AND http_status IN ('200','304')
 *     AND is_app_pageview(uri_path, uri_query, http_status, content_type, user_agent, x_analytics)
 *   GROUP BY
 *     LOWER(uri_host)
 *   ORDER BY cnt desc
 *   LIMIT 10
 *   ;
 */
@UDFType(deterministic = true)
public class IsAppPageviewUDF extends GenericUDF {

    private int maxArguments = 5;
    private int minArguments = 4;

    private boolean checkForXAnalytics = false;

    private ObjectInspector[] argumentsOI;

    /**
     * Executed once per job, checks arguments size.
     *
     * Accepts variable number of arguments, last argument being the
     * raw string that represents the xAnalytics map
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        GenericUDFHelper argsHelper = new GenericUDFHelper();
        //at least we should have 6 arguments
        argsHelper.checkArgsSize(arguments, minArguments, maxArguments);

        if (arguments.length > minArguments){
            checkForXAnalytics = true;
        }

        for (int i = 0; i < arguments.length; i++) {
            argsHelper.checkArgPrimitive(arguments, i);
        }

        argumentsOI = arguments;

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException{

        String uriPath = PrimitiveObjectInspectorUtils.getString(
            arguments[0].get(), (PrimitiveObjectInspector) argumentsOI[0]);
        String uriQuery = PrimitiveObjectInspectorUtils.getString(
            arguments[1].get(), (PrimitiveObjectInspector) argumentsOI[1]);

        String contentType = PrimitiveObjectInspectorUtils.getString(
            arguments[2].get(), (PrimitiveObjectInspector) argumentsOI[2]);
        String userAgent = PrimitiveObjectInspectorUtils.getString(
            arguments[3].get(), (PrimitiveObjectInspector) argumentsOI[3]);

        String rawXAnalyticsHeader = "";

        if (checkForXAnalytics) {
            rawXAnalyticsHeader = PrimitiveObjectInspectorUtils.getString(
                arguments[4].get(), (PrimitiveObjectInspector) argumentsOI[4]);
        }
        return PageviewDefinition.getInstance().isAppPageview(uriPath, uriQuery, contentType, userAgent, rawXAnalyticsHeader);

    }


    @Override
    public String getDisplayString(String[] arguments) {
        return "isAppPageView(" + arguments.toString() + ")";
    }
}
