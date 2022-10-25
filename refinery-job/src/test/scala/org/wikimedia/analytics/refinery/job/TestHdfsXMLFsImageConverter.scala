package org.wikimedia.analytics.refinery.job

import java.io.{BufferedReader, StringReader}

import com.ctc.wstx.stax.WstxInputFactory
import org.codehaus.stax2.XMLStreamReader2
import org.scalatest.{FlatSpec, Matchers}


class TestHdfsXMLFsImageConverter extends FlatSpec with Matchers {

  "HdfsXMLFsImageConverter" should "parse XML" in {
    val xml = """<inode>
      <id>134217736</id>
      <type>FILE</type>
      <name>000004_0</name>
      <replication>3</replication>
      <mtime>1477117366130</mtime>
      <atime>1597862082595</atime>
      <preferredBlockSize>268435456</preferredBlockSize>
      <permission>analytics:analytics:0755</permission>
      <blocks>
        <block>
          <id>1168840690</id>
          <genstamp>95130810</genstamp>
          <numBytes>49679794</numBytes>
        </block>
      </blocks>
      <storagePolicyId>0</storagePolicyId>
    </inode>"""


    val xmlInputFactory = new WstxInputFactory()
    val buffreader = new BufferedReader(new StringReader(xml))
    val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
    val inode = HdfsXMLFsImageConverter.Inode.parseXml(xmlReader)
    assert(inode.typ.get == "FILE")
  }

  "HdfsXMLFsImageConverter" should "parse XML of inode under construction" in {
      val xml = """<inode>
              <id>107413</id>
              <path>/tmp/hive/hive/017d847f-8190-4de2-879d-c41e1b43f804/inuse.lck</path>
          </inode>"""

      val xmlInputFactory = new WstxInputFactory()
      val buffreader = new BufferedReader(new StringReader(xml))
      val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
      val inode = HdfsXMLFsImageConverter.Inode.parseXml(xmlReader)
      assert(inode.typ.isEmpty)
  }

    "HdfsXMLFsImageConverter" should "parse XML of inode under construction within the INodeSection" in {
        val xml = """<inode>
            <id>107413</id>
            <type>FILE</type>
            <name>inuse.lck</name>
            <replication>3</replication>
            <mtime>1522925685856</mtime>
            <atime>1522925685856</atime>
            <perferredBlockSize>134217728</perferredBlockSize>
            <permission>hive:hdfs:rw-r--r--</permission>
            <file-under-construction>
                <clientName>DFSClient_NONMAPREDUCE_1373906289_1</clientName>
                <clientMachine>10.10.6.21</clientMachine>
            </file-under-construction>
        </inode>"""

        val xmlInputFactory = new WstxInputFactory()
        val buffreader = new BufferedReader(new StringReader(xml))
        val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
        val inode = HdfsXMLFsImageConverter.Inode.parseXml(xmlReader)
        assert(inode.fileUnderConstruction)
    }

    "HdfsXMLFsImageConverter" should "parse XML of a directory in the INodeDirectorySection" in {
        val xml = """<directory>
            <parent>107419</parent>
            <inode>107422</inode>
            <inode>107420</inode>
            <inode>107421</inode>
        </directory>"""

        val xmlInputFactory = new WstxInputFactory()
        val buffreader = new BufferedReader(new StringReader(xml))
        val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
        val directory = HdfsXMLFsImageConverter.Directory.parseXml(xmlReader)
        assert(directory.id.get == 107419)
    }

    "HdfsXMLFsImageConverter" should "parse XML of a directory in INodeSection" in {
        val xml = """<inode>
            <id>16386</id>
            <type>DIRECTORY</type>
            <name>tmp</name>
            <mtime>1521626967933</mtime>
            <permission>hive:hdfs:rwx-wx-wx</permission>
            <nsquota>-1</nsquota>
            <dsquota>-1</dsquota>
        </inode>"""

        val xmlInputFactory = new WstxInputFactory()
        val buffreader = new BufferedReader(new StringReader(xml))
        val xmlReader = xmlInputFactory.createXMLStreamReader(buffreader).asInstanceOf[XMLStreamReader2]
        val directory = HdfsXMLFsImageConverter.Inode.parseXml(xmlReader)
        assert(directory.typ.get == "DIRECTORY")
    }

}
