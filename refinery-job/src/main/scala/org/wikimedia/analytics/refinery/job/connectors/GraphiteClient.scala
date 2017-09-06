package org.wikimedia.analytics.refinery.job.connectors

import java.io.OutputStream
import java.net.Socket

import org.joda.time.DateTimeUtils

/**
 * Simple GraphiteClient in Scala
 * Creates a Socket and writes to it,
 * based on the plaintext protocol supported by Carbon
 *
 * See: http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol
 * for details
 *
 * Supports sendOnce to open a connection, send a message, close connection.
 * To write multiple messages at a time, something like
 *
 * val graphite = GraphiteClient('localhost')
 * val conn = graphite.connection()
 * m1 = graphite.message('foo.bar', 20)
 * m2 = graphite.message('foo.baz', 30)
 * conn.write(m1)
 * conn.write(m2)
 * conn.close()
 *
 */
class GraphiteClient(host:String, port: Int = 2003) {

  /**
   * Connection class that wraps the Socket creation,
   * and writing to the socket functionality, so the GraphiteClient
   * can be extended easily to support sending multiple messages, etc
   * @param host Graphite host url
   * @param port Graphite port
   */
  class Connection(host:String, port:Int) {
    val socket:Socket = new Socket(host, port)
    val out:OutputStream = socket.getOutputStream

    def write(data:String) = {
      out.write(data.getBytes())
      out.flush()
    }

    def close() = {
      out.close
      socket.close
    }
  }

  /**
   * Create an instance of Connection
   *
   * @return Instance of Connection that wraps the socket
   *         creation and writing to socket functionality
   */
  def connection() = {
    new Connection(host, port)
  }

  /**
   * Helper to create a message string following Carbon's plaintext protocol
   *
   * @param metric Name of the graphite metric, e.g foo.bar
   * @param value Value of the metric
   * @param timestamp Timestamp in seconds, defaults to current time
   * @return Formatted string for plaintext protocol
   */
  def message(metric:String, value:Long, timestamp:Long = DateTimeUtils.currentTimeMillis() / 1000) = {
    "%s %d %d\n".format(metric, value, timestamp)
  }

  /**
   * Helper that opens a connection, sends a message, and closes connection
   * @param metric Name of the graphite metric, e.g foo.bar
   * @param value Value of the metric
   * @param timestamp Timestamp in seconds
   */
  def sendOnce(metric:String, value:Long, timestamp:Long) = {
    val conn = connection()
    conn.write(message(metric, value, timestamp))
    conn.close()
  }

}
