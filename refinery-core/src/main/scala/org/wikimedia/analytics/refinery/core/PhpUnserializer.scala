package org.wikimedia.analytics.refinery.core

import scala.annotation.tailrec

/**
  * This code is a scala adaptation of the Locutus JavaScript Php Unserializer
  * http://locutus.io/php/var/unserialize/
  */
object PhpUnserializer extends Serializable {

  @tailrec
  private def readUntil(data: Array[Byte],
                        offset: Int,
                        stopChar: Char,
                        acc: Array[Byte] = Array.empty): Array[Byte] = {
    if (data(offset) == stopChar) acc
    else
      readUntil(data,
                offset + 1,
                stopChar,
                Array.concat(acc, Array(data(offset))))
  }

  private def readChars(data: Array[Byte],
                        offset: Int,
                        length: Int): Array[Byte] = {
    data.slice(offset, offset + length)
  }

  private def unserialize(data: Array[Byte],
                          offset: Int = 0): (Byte, Int, Any) = {
    //println("_unserialize", offset, data)
    val dataType = data(offset)
    val dataOffset = offset + 2

    def getNewOffsetAndValue(data: Array[Byte], offset: Int): (Int, Any) =
      dataType match {

        case 'i' =>
          val read = readUntil(data, offset, ';')
          (offset + read.length + 1, new String(read, "UTF-8").toInt)

        case 'b' =>
          val read = readUntil(data, offset, ';')
          (offset + read.length + 1, new String(read, "UTF-8").toInt != 0)

        case 'd' =>
          val read = readUntil(data, offset, ';')
          (offset + read.length + 1, new String(read, "UTF-8").toDouble)

        case 'N' => (offset, None)

        case 's' =>
          val value = readUntil(data, offset, ':')
          val stringOffset = offset + value.length + 2
          val read =
            readChars(data, stringOffset, new String(value, "UTF-8").toInt)
          (stringOffset + read.length + 2, new String(read, "UTF-8"))

        case 'a' =>
          val arrayKey = readUntil(data, offset, ':')
          val arrayKeyInt = new String(arrayKey, "UTF-8").toInt
          var newOffset = offset + arrayKey.length + 2
          var read = Map[String, Any]()

          (0 until arrayKeyInt).foreach(_ => {
            val keyProps = unserialize(data, newOffset)
            val key = keyProps._3.toString
            newOffset = newOffset + keyProps._2

            val valueProps = unserialize(data, newOffset)
            val value = valueProps._3
            newOffset = newOffset + valueProps._2

            read = read + (key -> value)
          })

          (newOffset + 1, read)

        case default =>
          throw new Exception(
              s"Type ${default.toChar} not supported in $data at position $offset")
      }

    val (newOffset, value) = getNewOffsetAndValue(data, dataOffset)
    (dataType, newOffset - offset, value)
  }

  def unserialize(phpSerialized: String): Any = {
    unserialize(phpSerialized.getBytes("UTF-8"), 0)._3
  }

}
