## Spark parsing of large XML files on HDFS

This package contains code to parse large XML files on HDFS in parallel.
It's using WoodStox StreamReader.
And it's allowing not to load the entire XML tree in memory (an HDFS fsimage
as XML is about 60 GB), but rather stream-read it.

This package contains a Hadoop InputFormat that reads the text between two XML tags.
HdfsXMLFsImageConverter uses it.

The `ByteMatcher` class and the `SeekableInputStream` class are low-level stream reading
and matching utilities, used by the core class to navigate the text-stream.