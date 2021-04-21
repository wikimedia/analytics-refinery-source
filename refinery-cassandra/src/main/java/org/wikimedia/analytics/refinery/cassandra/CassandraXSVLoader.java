/**
 * Copyright (C) 2015 Wikimedia Foundation
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

package org.wikimedia.analytics.refinery.cassandra;

import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;

import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 * Map-Reduce job loading data into a Cassandra cluster from separated-value files.
 *
 * Usage:
 * hadoop jar /path/to/refinery-job.jar org.wikimedia.analytics.refinery.cassandra.CassandraXSVLoader \
 *            -D mapreduce.job.user.classpath.first=true \
 *            -D mapreduce.job.reduces=X \
 *            -D cassandra_host=your_host \
 *            -D cassandra_username=aqsloader \
 *            -D cassandra_password=cassandra \
 *            -D input_path=/path/to/xsv/file/or/folder \
 *            -D input_separator=, \
 *            -D input_fields=text_field,,int_field,double_field \
 *            -D input_fields_types=text,,int,double \
 *            -D output_keyspace=keyspace1 \
 *            -D output_column_family=column_family1 \
 *            -D output_fields=text_field,constant_field \
 *            -D output_primary_keys=int_field,double_field
 *            -D constant_field=constant_value,text
 *
 * Input fields names and associated types must be given in the same order as in the file.
 * Accepted types are text, int, bigint, float, double, boolean.
 * Empty input fields and types are accepted for unused columns.
 *
 * Output keyspace and column family to be loaded must exist on the cluster.
 * Output values (primary keys or fields) must either be input fields
 * or defined as constant fields.
 *
 */
public class CassandraXSVLoader extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(CassandraXSVLoader.class);

    public static final String CASSANDRA_NODES = "cassandra_nodes";
    public static final String BATCH_SIZE_PROP = "batch_size";

    public static final String CASSANDRA_HOST_PROP = "cassandra_host";
    public static final String CASSANDRA_USER_PROP = "cassandra_username";
    public static final String CASSANDRA_PASSWD_PROP = "cassandra_password";

    public static final String INPUT_PATH_PROP = "input_path";
    public static final String OUTPUT_KEYSPACE_PROP = "output_keyspace";
    public static final String OUTPUT_COLUMN_FAMILY_PROP = "output_column_family";


    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new CassandraXSVLoader(), args);
        System.exit(0);
    }

    /**
     * Checks the input fields related parameters (fields and types)
     */
    private boolean checkInputFields(List<String> inputFieldsList, List<String> inputFieldsTypesList) {
        boolean result = true;

        // Check that input fields number == input fields type number
        if (inputFieldsList.size() != inputFieldsTypesList.size()) {
            logger.error("Incorrect configuration: Input fields number differ from input fields type number.");
            result = false;
        } else {

            // Check that every defined input field has a valid type
            for (int i = 0; i < inputFieldsList.size(); i++) {
                String inputField = inputFieldsList.get(i);
                String inputFieldType = inputFieldsTypesList.get(i);

                // If input field is defined
                if (! StringUtils.isEmpty(inputField)) {
                    // Check that input field type is known
                    try {
                        ReducerToCassandra.input_types.valueOf(inputFieldType.toUpperCase());
                    } catch (IllegalArgumentException _) {
                        logger.error("Incorrect configuration: Input field '" + inputField
                                + "' has unknown type '" + inputFieldType + "'.");
                        result = false;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Checks one output field (primary key or field) to be either an input field or a constant one.
     */
    private boolean checkOutputFieldOrPrimaryKey(String field, List<String> inputFieldsList,
                                                 Configuration conf, String logName) {
        boolean result = true;
        // Check that field is defined
        if (field.length() < 1) {
            logger.error("Incorrect configuration: " + logName + " undefined.");
            result = false;
        }
        if (! inputFieldsList.contains(field)) {
            // Field not from input --> Check it has a constant defined value in the form 'value,type'
            if (conf.get(field) != null) {
                List<String> confSetOutput = Arrays.asList(conf.get(field).split(","));
                if (confSetOutput.size() != 2) {
                    logger.error("Incorrect configuration: " + logName + " from configuration '" + field
                            + "' is not defined as 'value,type': " + conf.get(field));
                    result = false;
                } else {
                    // Check that field type is known
                    try {
                        ReducerToCassandra.input_types.valueOf(confSetOutput.get(1).toUpperCase());
                    } catch (IllegalArgumentException _) {
                         logger.error("Incorrect configuration: " + logName + " from configuration '" + field
                                 + "' has unknown type '" + confSetOutput.get(1) + "'.");
                         result = false;
                    }
                }
            } else {
                logger.error("Incorrect configuration: " + logName + " '" + field
                        + "' is not in input fields or conf.");
                result = false;
            }
        }
        return result;
    }

    /**
     * Checks parameters correctness.
     */
    public boolean checkConfParameters() {

        // Configuration from Tool
        Configuration conf = getConf();

        boolean parametersOk = true;

        // Check parameters are not empty
        if (StringUtils.isEmpty(conf.get(CASSANDRA_HOST_PROP))
                || StringUtils.isEmpty(conf.get(CASSANDRA_USER_PROP))
                || StringUtils.isEmpty(conf.get(CASSANDRA_PASSWD_PROP))
                || StringUtils.isEmpty(conf.get(INPUT_PATH_PROP))
                || StringUtils.isEmpty(conf.get(ReducerToCassandra.INPUT_SEPARATOR_PROP))
                || StringUtils.isEmpty(conf.get(ReducerToCassandra.INPUT_FIELDS_PROP))
                || StringUtils.isEmpty(conf.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP))
                || StringUtils.isEmpty(conf.get(OUTPUT_KEYSPACE_PROP))
                || StringUtils.isEmpty(conf.get(OUTPUT_COLUMN_FAMILY_PROP))
                || StringUtils.isEmpty(conf.get(ReducerToCassandra.OUTPUT_FIELDS_PROP))
                || StringUtils.isEmpty(conf.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP))) {
            logger.error("Incorrect configuration: Missing/empty parameter(s).");
            parametersOk = false;
        }

        if ((conf.get(ReducerToCassandra.INPUT_FIELDS_PROP) != null)
                && (conf.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP) != null)
                && (conf.get(ReducerToCassandra.OUTPUT_FIELDS_PROP) != null)
                && (conf.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP) != null)) {

            // Split parameters containing lists
            List<String> inputFieldsList = Arrays.asList(conf.get(ReducerToCassandra.INPUT_FIELDS_PROP).split(","));
            List<String> inputFieldsTypesList = Arrays.asList(conf.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP).split(","));
            List<String> outputFieldsList = Arrays.asList(conf.get(ReducerToCassandra.OUTPUT_FIELDS_PROP).split(","));
            List<String> primaryKeysList = Arrays.asList(conf.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP).split(","));

            // Check Input fields have correct types
            parametersOk = checkInputFields(inputFieldsList, inputFieldsTypesList) && parametersOk;

            // Check primary key fields are defined and present in input fields or conf
            for (String field : primaryKeysList) {
                parametersOk = checkOutputFieldOrPrimaryKey(field, inputFieldsList, conf, "Primary key") && parametersOk;
            }

            // Check output fields are defined and present in input or empty fields
            for (String field : outputFieldsList) {
                parametersOk = checkOutputFieldOrPrimaryKey(field, inputFieldsList, conf, "Output field") && parametersOk;
            }
        }
        return parametersOk;
    }

    /**
     * Quote identifier if not already done.
     */
    private String quoteIdentifier(String ident) {
        return (ident.startsWith("\"") && ident.endsWith("\"")) ? ident : "\"" + ident + "\"";
    }

    /**
     * Builds CQL update query based on output fields.
     */
    public String makeCqlQuery() {

        // Configuration from Tool
        Configuration conf = getConf();

        // Split output fields
        List<String> outputFieldsList = Arrays.asList(conf.get(ReducerToCassandra.OUTPUT_FIELDS_PROP).split(","));

        // Build cql query
        String cqlQuery = "UPDATE " + quoteIdentifier(conf.get(OUTPUT_KEYSPACE_PROP)) + "."
                + quoteIdentifier(conf.get(OUTPUT_COLUMN_FAMILY_PROP)) + " ";
        boolean first = true;
        for (String field: outputFieldsList) {
            String head;
            if (first) {
                head = "SET ";
                first = false;
            } else {
                head = ", ";
            }
            cqlQuery += head + "" + quoteIdentifier(field) + " = ?";
        }
        return cqlQuery;
    }

    /**
     * Runs the map-reduce job
     */
    public int run(String[] args) throws Exception {

        // Configuration from Tool
        Configuration conf = getConf();

        // Checking configuration parameters
        if (! checkConfParameters()) {
            logger.error("Problem with configuration, aborting.");
            System.exit(1);
        }

        //Build cql query
        String cqlQuery = makeCqlQuery();
        logger.info("CQL Query to be run: " + cqlQuery);

        // Parameters ok -> job configuration and launch
        Job job = new Job(conf, "CassandraXSVLoader");
        job.setJarByClass(CassandraXSVLoader.class);

        // Identity Mapper - Nothing to get done at map time
        job.setMapperClass(Mapper.class);
        FileInputFormat.addInputPath(job, new Path(conf.get(INPUT_PATH_PROP)));

        // reducer to cassandra
        job.setReducerClass(ReducerToCassandra.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Map.class);
        job.setOutputValueClass(List.class);

        // Use cassandra cql output format
        // This is where the actual connection and data push
        // to cassandra is made
        job.setOutputFormatClass(CqlOutputFormat.class);

        ConfigHelper.setOutputColumnFamily(
                job.getConfiguration(),
                quoteIdentifier(conf.get(OUTPUT_KEYSPACE_PROP)),
                quoteIdentifier(conf.get(OUTPUT_COLUMN_FAMILY_PROP)));

        CqlConfigHelper.setOutputCql(job.getConfiguration(), cqlQuery);
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        ConfigHelper.setOutputInitialAddress(
                job.getConfiguration(),
                conf.get(CASSANDRA_HOST_PROP));


        CqlConfigHelper.setUserNameAndPassword(
                job.getConfiguration(),
                conf.get(CASSANDRA_USER_PROP),
                conf.get(CASSANDRA_PASSWD_PROP));

        // Force cassandra to write LOCAL_QUORUM
        ConfigHelper.setWriteConsistencyLevel(job.getConfiguration(), "LOCAL_QUORUM");

        // If batch size parameters are set, use them
        if ((conf.getInt(CASSANDRA_NODES, 0) > 0) && (conf.getInt(BATCH_SIZE_PROP, 0) > 0)) {
            conf.setInt(CqlOutputFormat.BATCH_THRESHOLD, conf.getInt(BATCH_SIZE_PROP, 0));
            conf.setInt(CqlOutputFormat.QUEUE_SIZE,
                    conf.getInt(BATCH_SIZE_PROP, 0) * conf.getInt(CASSANDRA_NODES, 0) + 1);
        }

        job.waitForCompletion(true);
        return 0;
    }

}
