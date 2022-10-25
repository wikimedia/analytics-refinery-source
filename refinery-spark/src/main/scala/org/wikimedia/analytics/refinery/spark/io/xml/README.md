## Spark parsing of large XML files on HDFS

This package contains code to parse large XML files on HDFS in parallel.
It's using WoodStox StreamReader.
And it's allowing not to load the entire XML tree in memory (wiki dumps file can
be big !), but rather stream-read it.

This package contains code providing hadoop InputFormat for parsing
mediawiki XML-dumps, and more generally, utils to read some xml files on HDFS.

The `ByteMatcher` class and the `SeekableInputStream` class are low-level stream reading
and matching utilities, used by the core class to navigate the text-stream.