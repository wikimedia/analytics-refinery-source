package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.wikimedia.analytics.refinery.core.LocaleUtil;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZProjectConverter;

import java.util.List;

/**
 * A Hive UDF to return the standard wiki name from a Pagecounts-EZ-formatted name
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_country_name as 'org.wikimedia.analytics.refinery.hive.GetCountryNameUDF';
 *   SELECT convert_ez_project_to_standard("en.z");
 *
 */
@UDFType(deterministic = true)
@Description(
        name = "convert_ez_project_to_standard",
        value = "_FUNC_(wiki_project_in_pagecounts_ez_format) - returns the standard wiki name from the Pagecounts-EZ-formatted name provided",
        extended = "")
public class ConvertEZProjectToStandard extends GenericUDF {
    private final Text result = new Text();
    private StringObjectInspector argumentOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        GenericUDFHelper argsHelper = new GenericUDFHelper();
        argsHelper.checkArgsSize(arguments, 1, 1);
        argsHelper.checkArgPrimitive(arguments, 0);
        argsHelper.checkArgType(arguments, 0, PrimitiveObjectInspector.PrimitiveCategory.STRING);

        // Cache the argument to be used in evaluate
        argumentOI = (StringObjectInspector) arguments[0];

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.clear();
        String ezName = argumentOI.getPrimitiveJavaObject(arguments[0].get());
        String standardName = EZProjectConverter.ezProjectToStandard(ezName);
        result.set(new Text(standardName));
        return result;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "convert_ez_project_to_standard(" + arguments[0] + ")";
    }
}
