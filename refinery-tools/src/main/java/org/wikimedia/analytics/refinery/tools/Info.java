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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;

/**
 * Dumps information about a sequence file to stdout.
 */
public class Info {

    /**
     * Renders a row of key, and value for the user
     * @param key
     * @param value
     */
    private static void renderKeyValue(String key, String value) {
        System.out.println(key + ": " + value);
    }

    /**
     * Dumps information about a sequence file to stdout.
     *
     * @param args the first item is the filename of the file to get
     *     information about.
     * @throws IOException if file cannot be opened, read, ...
     */
    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            System.err.println("Usage: <file>\n"
                    + "\n"
                    + "<file> - file to get info about.");
            System.exit(1);
        }
        Path path = new Path(args[0]);
        Configuration conf = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path));

        String compressionType = reader.getCompressionType().toString();
        renderKeyValue("CompressionType", compressionType);

        String codecName = reader.getCompressionCodec().getClass().getName();
        String codecExt = reader.getCompressionCodec().getDefaultExtension();
        renderKeyValue("CompressionCodec", codecName + " (" + codecExt + ")");


        renderKeyValue("Key", reader.getKeyClassName());
        renderKeyValue("Value", reader.getValueClassName());

        Metadata metadata = reader.getMetadata();
        int metadataSize = metadata.getMetadata().size();
        renderKeyValue("Metadata", "(" + metadataSize + " metadata entries)");
        for (Entry<Text, Text> entry : metadata.getMetadata().entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            renderKeyValue(" * " + key, value);
        }

        reader.close();
    }
}
