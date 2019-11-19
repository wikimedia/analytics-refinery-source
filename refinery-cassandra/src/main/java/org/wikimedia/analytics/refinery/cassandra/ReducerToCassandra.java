package org.wikimedia.analytics.refinery.cassandra;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Reducer inserting into cassandra using
 */
public class ReducerToCassandra extends Reducer<LongWritable, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {

    private static final Logger logger = LoggerFactory.getLogger(ReducerToCassandra.class);


    public static final String INPUT_SEPARATOR_PROP = "input_separator";
    public static final String INPUT_FIELDS_PROP = "input_fields";
    public static final String INPUT_FIELDS_TYPES_PROP = "input_fields_types";

    public static final String OUTPUT_FIELDS_PROP = "output_fields";
    public static final String OUTPUT_PRIMARY_KEYS_PROP = "output_primary_keys";

    public enum input_types {
        TIMEUUID, TEXT, INT, BIGINT, VARINT, INT_NULLABLE, BIGINT_NULLABLE, FLOAT, DOUBLE, BOOLEAN
    }


    private String separator;
    private int inputFieldsNumber;
    private List<String> inputFieldsTypesList;
    private List<String> outputFieldsList;
    private List<String> primaryKeysList;
    private Map<String, Integer> inputFieldsIndexMapping;
    private Map<String, ByteBuffer> valuesFromConf;

    /**
     * Converts a value to a specific type and loads it into a ByteBuffer.
     */
    private ByteBuffer makeByteBufferedTypedValue(String value, String type) {
        try {
            switch (input_types.valueOf(type.toUpperCase())) {
                case TIMEUUID:
                    return ByteBufferUtil.bytes(UUIDs.startOf(Long.parseLong(value)));
                case TEXT:
                    return ByteBufferUtil.bytes(value);
                case INT:
                    return ByteBufferUtil.bytes(Integer.parseInt(value));
                case BIGINT:
                    return ByteBufferUtil.bytes(Long.parseLong(value));
                case VARINT:
                    return ByteBufferUtil.bytes(Long.parseLong(value));
                case INT_NULLABLE:
                    int i = Integer.parseInt(value);
                    if (i == 0)
                        return null;
                    else
                        return ByteBufferUtil.bytes(i);
                case BIGINT_NULLABLE:
                    long l = Long.parseLong(value);
                    if (l == 0L)
                        return null;
                    else
                        return ByteBufferUtil.bytes(l);
                case FLOAT:
                    return ByteBufferUtil.bytes(Float.parseFloat(value));
                case DOUBLE:
                    return ByteBufferUtil.bytes(Double.parseDouble(value));
                case BOOLEAN:
                    if (Boolean.parseBoolean(value))
                        return ByteBufferUtil.bytes("true");
                    else
                        return ByteBufferUtil.bytes("false");
            }
        } catch (Exception e) {
            logger.error("Fatal error: value '" + value + "' can't be converted to type '" + type + "'.");
            throw e;
        }
        // Will never be returned
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }


    /**
     * Prepares the reducer from configuration parameters
     */
    @Override
    protected void setup(Context context) {
        // Configuration from context
        Configuration conf = context.getConfiguration();

        // Parameter correctness is checked before job launched
        // Just do it !
        separator = conf.get(INPUT_SEPARATOR_PROP);
        List<String> inputFieldsList = Arrays.asList(conf.get(INPUT_FIELDS_PROP).split(","));
        inputFieldsTypesList = Arrays.asList(conf.get(INPUT_FIELDS_TYPES_PROP).split(","));
        outputFieldsList = Arrays.asList(conf.get(OUTPUT_FIELDS_PROP).split(","));
        primaryKeysList = Arrays.asList(conf.get(OUTPUT_PRIMARY_KEYS_PROP).split(","));

        inputFieldsNumber = inputFieldsList.size();
        inputFieldsIndexMapping = new HashMap<>(primaryKeysList.size());
        valuesFromConf = new HashMap<>();

        Set<String> allOutputs = new HashSet();
        allOutputs.addAll(primaryKeysList);
        allOutputs.addAll(outputFieldsList);

        // Input field mapping or conf set value
        for (String field : allOutputs) {
            if (inputFieldsList.contains(field)) {
                inputFieldsIndexMapping.put(field, inputFieldsList.indexOf(field));
            } else {
                // Input field index mapping set to -1 and prepare value from conf
                inputFieldsIndexMapping.put(field, -1);
                String[] valueType = conf.get(field).split(",");
                valuesFromConf.put(field, makeByteBufferedTypedValue(valueType[0], valueType[1]));
            }
        }
    }


    /**
     * Split given lines and write prepared data for cassandra loading based on configuration.
     */
    @Override
    public void reduce(LongWritable index, Iterable<Text> lines, Context context) throws IOException, InterruptedException {

        // In case multiple lines have the same index in file
        for (Text line : lines) {
            Map<String, ByteBuffer> keys = new LinkedHashMap<>();
            List<ByteBuffer> variables = new ArrayList<>();

            String[] splittedLine = line.toString().split(separator);

            // Don't work line if it doesn't contain enough values
            if (splittedLine.length < inputFieldsNumber) {
                logger.warn("Line contained less values than needed input fields '" + line + "'.");
                continue;
            }

            // Setting keys
            for (String primaryKey : primaryKeysList) {
                int inputFieldIndex = inputFieldsIndexMapping.get(primaryKey);
                if (inputFieldIndex < 0) {  // field from conf
                    keys.put(primaryKey, valuesFromConf.get(primaryKey));
                } else {                    // field from input
                    keys.put(primaryKey, makeByteBufferedTypedValue(
                            splittedLine[inputFieldIndex], inputFieldsTypesList.get(inputFieldIndex)));
                }

            }

            // Setting variables
            for (String outputField : outputFieldsList) {
                int inputFieldIndex = inputFieldsIndexMapping.get(outputField);
                if (inputFieldIndex < 0) {  // field from conf
                    variables.add(valuesFromConf.get(outputField));
                } else {                    // field from input
                    variables.add(makeByteBufferedTypedValue(splittedLine[inputFieldIndex],
                            inputFieldsTypesList.get(inputFieldIndex)));
                }

            }

            // Write output, in case of cassandra woes uncomment debug line
            // logging setup of cassandra-all package is such that debug is the
            // default login level
            //logger.debug("Writing new result for line '" + line + "'");
            context.write(keys, variables);
        }
    }

}
