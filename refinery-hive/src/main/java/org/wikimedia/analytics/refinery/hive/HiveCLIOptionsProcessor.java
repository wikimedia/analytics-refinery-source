/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.hive;

import java.util.HashMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.cli.CommonCliOptions;

/**
 * This code is an updated copy of {{@link org.apache.hadoop.hive.cli.OptionsProcessor}}
 * It is needed to facilitate expending parameters parsing when reusing our
 * {{@link org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver}}.
 */
public class HiveCLIOptionsProcessor {
    private static final String HIVECONF = "hiveconf";

    // Removed the final aspect of the Options
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    Map<String, String> hiveVariables = new HashMap<>();

    /**
     * Allows adding an Option to the options parsed by the OptionsProcessor.
     * @param opt the option to add
     */
    public final void addOption(Option opt) {
        this.options.addOption(opt);
    }

    @SuppressWarnings("static-access")
    public HiveCLIOptionsProcessor() {

        // -database database
        this.addOption(OptionBuilder
                .hasArg()
                .withArgName("databasename")
                .withLongOpt("database")
                .withDescription("Specify the database to use")
                .create());

        // -e 'quoted-query-string'
        this.addOption(OptionBuilder
                .hasArg()
                .withArgName("quoted-query-string")
                .withDescription("SQL from command line")
                .create('e'));

        // -f <query-file>
        this.addOption(OptionBuilder
                .hasArg()
                .withArgName("filename")
                .withDescription("SQL from files")
                .create('f'));

        // -i <init-query-file>
        this.addOption(OptionBuilder
                .hasArg()
                .withArgName("filename")
                .withDescription("Initialization SQL file")
                .create('i'));

        // -hiveconf x=y
        this.addOption(OptionBuilder
                .withValueSeparator()
                .hasArgs(2)
                .withArgName("property=value")
                .withLongOpt(HIVECONF)
                .withDescription("Use value for given property")
                .create());

        // Substitution option -d, --define
        this.addOption(OptionBuilder
                .withValueSeparator()
                .hasArgs(2)
                .withArgName("key=value")
                .withLongOpt("define")
                .withDescription("Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B")
                .create('d'));

        // Substitution option --hivevar
        this.addOption(OptionBuilder
                .withValueSeparator()
                .hasArgs(2)
                .withArgName("key=value")
                .withLongOpt("hivevar")
                .withDescription("Variable substitution to apply to Hive commands. e.g. --hivevar A=B")
                .create());

        // [-S|--silent]
        this.addOption(new Option("S", "silent", false, "Silent mode in interactive shell"));

        // [-v|--verbose]
        this.addOption(new Option("v", "verbose", false, "Verbose mode (echo executed SQL to the console)"));

        // [-H|--help]
        this.addOption(new Option("H", "help", false, "Print help information"));

    }

    // Extract the parseArgs function to decouple parameter parsing from SQL execution
    public boolean parseArgs(String[] argv) {
        if (Arrays.asList(argv).contains("--help")) {
            printUsage();
            System.exit(0);
        }
        try {
            commandLine = new GnuParser().parse(options, argv);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printUsage();
            return false;
        }
        return true;
    }

    public void process_stage1() {
        Properties confProps = commandLine.getOptionProperties(HIVECONF);
        for (String propKey : confProps.stringPropertyNames()) {
            // with HIVE-11304, hive.root.logger cannot have both logger name and log level.
            // if we still see it, split logger and level separately for hive.root.logger
            // and hive.log.level respectively
            if (propKey.equalsIgnoreCase("hive.root.logger")) {
                CommonCliOptions.splitAndSetLogger(propKey, confProps);
            } else {
                System.setProperty(propKey, confProps.getProperty(propKey));
            }
        }

        Properties hiveVars = commandLine.getOptionProperties("define");
        for (String propKey : hiveVars.stringPropertyNames()) {
            hiveVariables.put(propKey, hiveVars.getProperty(propKey));
        }

        Properties hiveVars2 = commandLine.getOptionProperties("hivevar");
        for (String propKey : hiveVars2.stringPropertyNames()) {
            hiveVariables.put(propKey, hiveVars2.getProperty(propKey));
        }
    }

    public boolean process_stage2(CliSessionState ss) {
        ss.getConf();

        if (commandLine.hasOption('H')) {
            printUsage();
            return false;
        }

        ss.setIsSilent(commandLine.hasOption('S'));

        ss.database = commandLine.getOptionValue("database");

        ss.execString = commandLine.getOptionValue('e');

        ss.fileName = commandLine.getOptionValue('f');

        ss.setIsVerbose(commandLine.hasOption('v'));

        String[] initFiles = commandLine.getOptionValues('i');
        if (null != initFiles) {
            ss.initFiles = Arrays.asList(initFiles);
        }

        if (ss.execString != null && ss.fileName != null) {
            System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
            printUsage();
            return false;
        }

        if (commandLine.hasOption(HIVECONF)) {
            Properties confProps = commandLine.getOptionProperties(HIVECONF);
            for (String propKey : confProps.stringPropertyNames()) {
                ss.cmdProperties.setProperty(propKey, confProps.getProperty(propKey));
            }
        }

        return true;
    }

    public void printUsage() {
        new HelpFormatter().printHelp("hive", options);
    }

    public Map<String, String> getHiveVariables() {
        return hiveVariables;
    }

    public CommandLine getCommandLine() {
        return commandLine;
    }
}
