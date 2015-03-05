// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.tools.guard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

public abstract class StdinGuard {
    /**
     * Runs checks for a single line for a line
     *
     * @param line the line to train with
     * @throws GuardException if the check fails
     */
    protected abstract void check(String line) throws GuardException;

    protected InputStream stdin = System.in;
    protected PrintStream stderr = System.err;

    @Option(name = "--help", aliases = {"-help", "-h", "-?"}, help = true,
            usage = "print this help screen")
    private boolean help;

    protected void exit(int status) {
        System.exit(status);
    };

    private void parseArgs(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(stderr);
            exitWithException("Failed to parse args", e);
        }

        if (help) {
            // User asked for help screen
            parser.printUsage(stderr);
            stderr.println();
            stderr.print(parser.printExample(OptionHandlerFilter.REQUIRED));
            exit(0);
        }
    }

    private void exitWithException(String reason, Exception e) {
        if (reason != null) {
            stderr.println(reason);
        }
        e.printStackTrace(stderr);
        exit(1);
    }

    protected void doMain(String[] args) {
        parseArgs(args);

        BufferedReader in =
                new BufferedReader(new InputStreamReader(stdin));

        String line = null;
        try {
            while ((line = in.readLine()) != null) {
                try {
                    check(line);
                } catch (GuardException te) {
                    exitWithException(null, te);
                }
            }
        } catch (IOException e) {
            exitWithException("Failed while reading next line (last line: '"
                    + line + "')", e);
        }
        exit(0);
    }
}
