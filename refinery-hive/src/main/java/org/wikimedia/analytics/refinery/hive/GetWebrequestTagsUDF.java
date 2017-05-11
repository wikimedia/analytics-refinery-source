package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;
import org.wikimedia.analytics.refinery.core.webrequest.tag.TaggerChain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

@UDFType(deterministic = true)
@Description(name = "tag", value = "_FUNC_(UA) - "
    + "Returns an array of tags for a given request")

// for CR: how about one UDF extending another one?
public class GetWebrequestTagsUDF extends IsPageviewUDF{

    private TaggerChain taggerChain;

    /**
     * Executed once per job, checks arguments size.
     * Initializes the chain of taggers that can return a possible tag for the request
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException{

        try {
            this.taggerChain = new TaggerChain();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

        super.initialize(arguments);
        return ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException{

        Set<String> tags = new HashSet<>();

        String uriHost = PrimitiveObjectInspectorUtils.getString(
            arguments[0].get(), (PrimitiveObjectInspector) argumentsOI[0]);

        String uriPath = PrimitiveObjectInspectorUtils.getString(
            arguments[1].get(), (PrimitiveObjectInspector) argumentsOI[1]);

        String uriQuery = PrimitiveObjectInspectorUtils.getString(
            arguments[2].get(), (PrimitiveObjectInspector) argumentsOI[2]);

        String httpStatus = PrimitiveObjectInspectorUtils.getString(
            arguments[3].get(), (PrimitiveObjectInspector) argumentsOI[3]);

        String contentType = PrimitiveObjectInspectorUtils.getString(
            arguments[4].get(), (PrimitiveObjectInspector) argumentsOI[4]);

        String userAgent = PrimitiveObjectInspectorUtils.getString(
            arguments[5].get(), (PrimitiveObjectInspector) argumentsOI[5]);

        String rawXAnalyticsHeader = "";

        if (checkForXAnalytics) {
            rawXAnalyticsHeader = PrimitiveObjectInspectorUtils.getString(
                arguments[6].get(), (PrimitiveObjectInspector) argumentsOI[6]);
        }


        WebrequestData webrequest = new WebrequestData(uriHost, uriPath, uriQuery,
            httpStatus, contentType, userAgent, rawXAnalyticsHeader);

        // converting set to a list
        tags = taggerChain.getTags(webrequest);

        return new ArrayList<String>(tags);


    }

    @Override
    public String getDisplayString(String[] arguments){
        return "GetWebrequestTagsUDF(" + arguments.toString() + ")";
    }
}