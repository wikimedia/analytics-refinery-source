package org.wikimedia.analytics.refinery.job.mediawikihistory.utils

import org.scalatest.{FlatSpec, Matchers}

class TestPhpUnserializer extends FlatSpec with Matchers {

  "A PhpUnserializer" should "unserializer a set of examples" in {

    val phpStrings: Seq[String] = Seq(
        """a:2:{s:9:"4::target";s:19:"User:\uD800\uDF37u\uD800\uDF3Ca\uD800\uDF3D";s:10:"5::noredir";s:1:"0";}""",
        """a:2:{s:9:"4::target";s:39:"RPR болон DPT –н ойлголт";s:10:"5::noredir";s:1:"0";}""",
        """a:4:{s:7:"4::type";s:8:"revision";s:6:"5::ids";a:5:{i:0;s:7:"5304167";i:1;s:7:"5304152";i:2;s:7:"5304147";i:3;s:7:"5304142";i:4;s:7:"5304123";}s:9:"6::ofield";i:0;s:9:"7::nfield";i:1;}""",
        """a:4:{i:0;i:1;i:1;N;i:2;s:8:"mystring";i:3;a:1:{s:3:"key";s:5:"value";}}""",
        """a:6:{i:10;i:1234;d:123.5;i:12;s:5:"first";a:1:{s:5:"inner";a:1:{s:2:"hi";s:3:"bye";}}s:4:"true";b:0;s:11:"hello world";N;s:3:"ids";a:10:{i:0;i:1;i:1;i:2;i:2;i:3;i:3;i:4;i:4;i:5;i:5;i:6;i:6;i:7;i:7;i:8;i:8;i:9;i:9;i:10;}}""",
        """a:2:{s:9:"4::target";s:13:"Yasujirō Ozu";s:10:"5::noredir";s:1:"0";}""",
        """a:2:{s:9:"4::target";s:19:"Talk:Joaqun Guzmán";s:10:"5::noredir";s:1:"0";}""",
        """a:2:{s:9:"4::target";s:31:"El Niño–Southern Oscillation";s:10:"5::noredir";s:1:"0";}"""
    )
    phpStrings.foreach(str => {
      val phpParsedObject = PhpUnserializer.unserialize(str)
      phpParsedObject should not be null.asInstanceOf[Any]
    })
  }

}
