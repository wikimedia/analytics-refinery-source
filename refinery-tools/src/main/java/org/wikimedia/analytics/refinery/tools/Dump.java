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

package org.wikimedia.analytics.refinery.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

/**
 * Dumps a sequence file to stdout.
 * <p>
 * Each value of the sequence file is printed on a separate line.
 */
public class Dump {
    /**
     * Dumps a sequence file and dumps it to stdout.
     * @param args the first item is used as name of the file be read.
     * @throws IOException if whatever IO problem occurs.
     * @throws IllegalAccessException if instantiating Writables fails.
     * @throws InstantiationException if instantiating Writables fails.
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {
        if (args == null || args.length != 1) {
            System.err.println("Usage: <file>\n" + "\n"
                    + "<file> - Read this file as a Hadoop SequenceFile and output text to stdout.");
            System.exit(1);
        }

        Path path = new Path(args[0]);

        Configuration conf = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path));
        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
                String valueStr = value.toString();
                if (valueStr.endsWith("\n")) {
                    System.out.print(valueStr);
                } else {
                    System.out.println(valueStr);
                }
            }
        } finally {
            reader.close();
        }
    }
}
