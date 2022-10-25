# Mediawiki XML-dumps parsing

The `MediawikiXMLRevisionInputFormat` is the core abstract class of this package.
The abstract functions defined in `KeyValueFactory` are the building of  the record
key and value out of parsed data, and a filtering function.
Having them abstract allows to easily instantiate different InputFormat in terms of
generated key-value types and datasets without changing the core class.
The types parameters to provide to the core class are:
* K - The type of the record key
* V - The type of the record value
* MwObjectsFactory - The type of the class implementing `MediawikiObjectsFactory`
  for mediawiki-objects manipulation

Finally, using the `MediawikiXMLRevisionInputFormat` class as base and implementing the
missing functions from `KeyValueFactory`, a set of XML-to-JSON InputFormat classes are
provided in the `MediawikiXMLRevisionToJSONInputFormats` file.

This package needs `org.wikimedia.analytics.refinery.spark.io.xml`, a common library
to parse large XML files on HDFS in parallel. It's using WoodStox StreamReader.

The parsing is triggered from in the `MediawikiXMLParser` class.
The parsed-objects (revision, page and user) are abstracted in the `MediawikiObjectsFactory`
trait. This abstraction allows to pick underlying representation to match the needs.
Currently, provided implementations are `MediawikiObjectsMapFactory` using Map[String, Any]
and `MediawikiObjectsCaseClassesFactory` using scala case-classes.