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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * A Hive UDF to classify a Wikimedia webrequest as a 'pageview'.
 * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
 * for information on how to classify a pageview.
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
 * webrequest_source = 'mobile'
 * AND year=2014
 * AND month=12
 * AND day=7
 * AND hour=12
 * AND is_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, <x_analytics_map></x_analytics_map>)
 * GROUP BY
 * LOWER(uri_host)
 * ORDER BY cnt desc
 * LIMIT 10
 * ;
 */
@Description(name = "is_pageview",
        value = "_FUNC_(uri_host, uri_path, uri_query, http_status, content_type, user_agent) - Returns true if the request is a pageview",
        extended = "")
public class IsPageviewUDF extends GenericUDF {

    private ObjectInspector[] argumentsOI;
    private transient MapObjectInspector mapOI;

    PageviewDefinition pageviewDefinitionInstance;

    boolean checkForXAnalytics = false;
    int maxArguments = 7;
    int minArguments = 6;

    /**
     * Executed once per job, checks arguments size.
     *
     * In order for changes to be backwards compatible when adding x_analytics_map
     * accepts that this argument might not be present, thus number
     * of arguments is variable.
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

        for (int i = 0; i < minArguments - 1; i++) {
            checkArgPrimitive(arguments, i);
        }


        if (checkForXAnalytics && arguments[arguments.length - 1].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException("Last Argument should be x_analytics map");
        }

        argumentsOI = arguments;
        pageviewDefinitionInstance = PageviewDefinition.getInstance();


        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String separator = PrimitiveObjectInspectorUtils.getString(
                arguments[0].get(), (PrimitiveObjectInspector) argumentsOI[0]);

        String uriHost = PrimitiveObjectInspectorUtils.getString(
                arguments[0].get(), (PrimitiveObjectInspector) argumentsOI[1]);
        String uriPath = PrimitiveObjectInspectorUtils.getString(
                arguments[1].get(), (PrimitiveObjectInspector) argumentsOI[2]);
        String uriQuery = PrimitiveObjectInspectorUtils.getString(
                arguments[2].get(), (PrimitiveObjectInspector) argumentsOI[3]);
        ;
        String httpStatus = PrimitiveObjectInspectorUtils.getString(
                arguments[3].get(), (PrimitiveObjectInspector) argumentsOI[4]);
        String contentType = PrimitiveObjectInspectorUtils.getString(
                arguments[4].get(), (PrimitiveObjectInspector) argumentsOI[5]);
        String userAgent = PrimitiveObjectInspectorUtils.getString(
                arguments[5].get(), (PrimitiveObjectInspector) argumentsOI[6]);

        Map<String, String> xAnalyticsMap = new HashMap<String, String>();

        if (checkForXAnalytics) {
            Map<?, ?> xAnalyticsPreMap = ((MapObjectInspector) argumentsOI[7]).getMap(arguments[7]);


            if (xAnalyticsPreMap != null) {

                for (Entry<?, ?> r : xAnalyticsMap.entrySet()) {
                    xAnalyticsMap.put((String) r.getKey(), (String) r.getValue());

                }
            }
        }
        return pageviewDefinitionInstance.isPageview(
                uriHost,
                uriPath,
                uriQuery,
                httpStatus,
                contentType,
                userAgent,
                xAnalyticsMap
        );
    }


    @Override
    public String getDisplayString(String[] arguments) {
        return "isPageView(" + arguments.toString() + ")";
    }


    //these two are package protected in GenericUDF.java but are real useful

    protected void checkArgsSize(ObjectInspector[] arguments, int min, int max)
            throws UDFArgumentLengthException {
        if (arguments.length < min || arguments.length > max) {
            StringBuilder sb = new StringBuilder();
            sb.append(getFuncName());
            sb.append(" requires ");
            if (min == max) {
                sb.append(min);
            } else {
                sb.append(min).append("..").append(max);
            }
            sb.append(" argument(s), got ");
            sb.append(arguments.length);
            throw new UDFArgumentLengthException(sb.toString());
        }
    }

    protected void checkArgPrimitive(ObjectInspector[] arguments, int i)
            throws UDFArgumentTypeException {
        ObjectInspector.Category oiCat = arguments[i].getCategory();
        if (oiCat != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(i, getFuncName() + " Argument should be of primitive type");
        }
    }

    protected String getFuncName() {
        return
                getClass().getSimpleName().substring(10).toLowerCase();
    }
}