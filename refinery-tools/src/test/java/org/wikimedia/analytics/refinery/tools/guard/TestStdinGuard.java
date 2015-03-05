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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

public class TestStdinGuard extends TestCase {

    public void testEmpty() {
        List<String> lines = new LinkedList<String>();

        StdinGuardShim guard = new StdinGuardShim(lines);
        guard.doMain(new String[]{});

        guard.assertStderrIsEmpty();

        guard.assertExitStatus(0);
    }

    public void testSingleOk() {
        List<String> lines = new LinkedList<String>();
        lines.add("ok");

        StdinGuardShim guard = new StdinGuardShim(lines);
        guard.doMain(new String[]{});

        guard.assertStderrIsEmpty();

        guard.assertExitStatus(0);
    }

    public void testSingleFail() {
        List<String> lines = new LinkedList<String>();
        lines.add("fail:foo");

        StdinGuardShim guard = new StdinGuardShim(lines);
        guard.doMain(new String[]{});

        guard.assertExitStatus(1);

        guard.assertStderrContains("foo");
    }

    public void testHelpPage() {
        List<String> lines = new LinkedList<String>();

        StdinGuardShim guard = new StdinGuardShim(lines);
        guard.doMain(new String[]{"--help"});

        guard.assertExitStatus(0);

        guard.assertStderrContains("print this");
    }

    /**
     * Exception thrown by the StdinGuardShim when simulating System.exit
     */
    class ShimExitException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Shim for StdinGuard to make relevant aspects testable
     */
    class StdinGuardShim extends StdinGuard {
        private Integer exitStatus = null;
        private ByteArrayOutputStream stderrStream;

        public StdinGuardShim(List<String> lines) {
            StringBuffer sb = new StringBuffer();
            String connector = "";
            for (String line : lines) {
                sb.append(connector);
                sb.append(line);
                connector = "\n";
            }
            stdin = new ByteArrayInputStream(sb.toString().getBytes());

            stderrStream = new ByteArrayOutputStream();
            stderr = new PrintStream(stderrStream);

        }

        @Override
        protected void doMain(String[] args) {
            try {
                super.doMain(args);
            } catch (ShimExitException e) {
            }
        }

        @Override
        protected void check(String line) throws GuardException {
            if (line.startsWith("fail:")) {
                throw new GuardException(line.substring(5));
            }
        }

        @Override
        protected void exit(int status) {
            assertNull("exit status has already been set!", exitStatus);
            exitStatus = status;
            throw new ShimExitException();
        }

        public void assertExitStatus(int status) {
            assertEquals("Wrong shim exit status",
                    new Integer(status), exitStatus);
        }

        public void assertStderrIsEmpty() {
            stderr.flush();
            String stderrStr = stderrStream.toString();
            assertTrue("Stderr is '" + stderrStr + "', but should be empty",
                    stderrStr.isEmpty());
        }

        public void assertStderrContains(String str) {
            stderr.flush();
            String stderrStr = stderrStream.toString();
            assertTrue("Stderr is '" + stderrStr + "', but should contain '"
                    + str + "'",
                    stderrStr.contains(str));
        }
    }
}
