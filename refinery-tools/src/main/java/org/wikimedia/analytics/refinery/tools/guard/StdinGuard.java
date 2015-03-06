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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    @Option(name = "--failure-limit-total", metaVar="N",
            usage = "only fail if more than N failures occur. (default: 0)")
    private int failureLimitTotal = 0;

    @Option(name = "--failure-limit-per-kind", metaVar="N",
            usage = "only fail if more than N failures per kind occur. "
                    + "(default: 0)")
    private int failureLimitPerKind = 0;

    Map<String, List<Exception>> failures = new HashMap<String,
            List<Exception>>();

    int failuresTotal = 0;

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

    private void addFailure(Exception e) {
        String kind = e.getMessage();
        List<Exception> failuresPerKind = failures.get(kind);
        if (failuresPerKind == null) {
            failuresPerKind = new LinkedList<Exception>();
            failures.put(kind, failuresPerKind);
        }
        failuresPerKind.add(e);
        failuresTotal++;

        int failuresPerKindCount = failuresPerKind.size();
        if (failuresPerKindCount > failureLimitPerKind) {
            exitExceptionLimit("Failures per kind > limit ( "
                    + failuresPerKindCount + " > "+ failureLimitPerKind
                    + " ) for '" + kind + "'");
        }

        if (failuresTotal > failureLimitTotal) {
            exitExceptionLimit("Total failure lines > limit ( "
                    + failuresTotal + " > "+ failureLimitTotal + " )");
        }
    }

    private void exitExceptionLimit(String reason) {
        if (reason != null) {
            stderr.println(reason);
        }
        for (List<Exception> failuresPerKind : failures.values()) {
            for (Exception failure : failuresPerKind) {
                stderr.println(failure.getMessage());
            }
        }
        exit(1);
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
                    addFailure(te);
                }
            }
        } catch (IOException e) {
            exitWithException("Failed while reading next line (last line: '"
                    + line + "')", e);
        }
        exit(0);
    }
}
