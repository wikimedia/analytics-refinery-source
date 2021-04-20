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
import org.apache.hadoop.io.Text;
import org.wikimedia.analytics.refinery.core.pagecountsEZ.EZProjectConverter;

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
    private Converter[] converters = new Converter[1];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

    private final Text result = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 1);
        checkArgPrimitive(arguments, 0);
        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        obtainStringConverter(arguments, 0, inputTypes, converters);

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.clear();
        String ezName = getStringValue(arguments, 0, converters);
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
