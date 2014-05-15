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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Stores data into a snappy block-compressed sequence file.
 * <p>
 * Data to store is read line-by-line from stdin. Each line is treated as
 * separate value. Keys are increasing, starting by 0.
 */
public class Store {
    /**
     * Stores data into a snappy block-compressed sequence file.
     *
     * @param args
     *            the first item is used as filename for the destination
     *            sequence file
     * @throws IOException
     *             if whatever IO problem occurs
     */
    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            System.err.println("Usage: <file>\n" + "\n"
                    + "<file> - store stdin as SequenceFile into this file.");
            System.exit(1);
        }

        Path path = new Path(args[0]);
        System.err.println("Reading from stdin, storing as " + path);

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));

        Configuration conf = new Configuration();
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(Text.class),
                SequenceFile.Writer.compression(
                        SequenceFile.CompressionType.BLOCK,
                        new org.apache.hadoop.io.compress.SnappyCodec()));

        try {
            String line;
            long key = 0;

            // We loop over the lines and append them to the writer.
            //
            // Note that we do not treat the line's trailing newline as part of
            // the line.
            while ((line = reader.readLine()) != null) {
                writer.append(new LongWritable(key++), new Text(line));
            }
        } finally {
            writer.close();
        }
    }
}
