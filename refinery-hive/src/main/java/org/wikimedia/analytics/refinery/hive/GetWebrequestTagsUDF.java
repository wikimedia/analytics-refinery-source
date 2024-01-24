package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;
import org.wikimedia.analytics.refinery.core.webrequest.tag.TaggerChain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@UDFType(deterministic = true)
@Description(name = "tag", value = "_FUNC_(UA) - "
    + "Returns an array of tags for a given request")

// for CR: how about one UDF extending another one?
public class GetWebrequestTagsUDF extends IsPageviewUDF{

    private TaggerChain taggerChain;
    private MapObjectInspector mapInspector;

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

        mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector
        );

        super.initialize(arguments);
        return ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException{

        Set<String> tags;

        String uriHost = getStringValue(arguments, 0, converters);
        String uriPath = getStringValue(arguments, 1, converters);
        String uriQuery = getStringValue(arguments, 2, converters);
        String httpStatus = getStringValue(arguments, 3, converters);
        String contentType = getStringValue(arguments, 4, converters);
        String userAgent = getStringValue(arguments, 5, converters);

        Map<String, String> xAnalyticsHeader;

        if (checkForXAnalytics && mapInspector.getMapSize(arguments[6].get()) > 0) {
            @SuppressWarnings("unchecked") Map<Object, Object> map = (Map<Object, Object>) mapInspector.getMap(arguments[6].get());
            xAnalyticsHeader = convertMapToMapOfStrings(map);
        } else {
            xAnalyticsHeader = new HashMap<>();
        }

        WebrequestData webrequest = new WebrequestData(uriHost, uriPath, uriQuery,
            httpStatus, contentType, userAgent, xAnalyticsHeader);

        // converting set to a list
        tags = taggerChain.getTags(webrequest);

        return new ArrayList<>(tags);
    }

    @Override
    public String getDisplayString(String[] arguments){
        return "GetWebrequestTagsUDF(" + arguments.toString() + ")";
    }
}
