/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.spark.bzip2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.bzip2.BZip2Constants;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This code is shamelessly copied and modified from hadoop-common trunk/release-2.6.0.
 * It extends the original [[BZip2Codec]] class to use the class that actually been fixed:
 * [[CorrectedBZip2CompressionInputStream]].
 * The [[CRC]] class has been copied since it is package-private in the original code.
 *
 * To be used with setting:
 *   * Hadoop:
 *      -Dhadoop.io.compression.codecs=org.wikimedia.analytics.refinery.spark.bzip2.CorrectedBZip2Codec
 *   * Spark:
 *      --conf spark.hadoop.io.compression.codecs=org.wikimedia.analytics.refinery.spark.bzip2.CorrectedBZip2Codec
 */

/**
 * This class provides output and input streams for bzip2 compression
 * and decompression.  It uses the native bzip2 library on the system
 * if possible, else it uses a pure-Java implementation of the bzip2
 * algorithm.  The configuration parameter
 * io.compression.codec.bzip2.library can be used to control this
 * behavior.
 *
 * In the pure-Java mode, the Compressor and Decompressor interfaces
 * are not implemented.  Therefore, in that mode, those methods of
 * CompressionCodec which have a Compressor or Decompressor type
 * argument, throw UnsupportedOperationException.
 *
 * Currently, support for splittability is available only in the
 * pure-Java mode; therefore, if a SplitCompressionInputStream is
 * requested, the pure-Java implementation is used, regardless of the
 * setting of the configuration parameter mentioned above.
 */
public class CorrectedBZip2Codec extends org.apache.hadoop.io.compress.BZip2Codec {

  private static final String HEADER = "BZ";
  private static final int HEADER_LEN = HEADER.length();
  private static final String SUB_HEADER = "h9";
  private static final int SUB_HEADER_LEN = SUB_HEADER.length();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return super.getConf();
  }

  /**
   * Creates a new instance of CorrectedBZip2Codec.
   */
  public CorrectedBZip2Codec() { }



  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}, and return a 
   * stream for uncompressed data.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    return Bzip2Factory.isNativeBzip2Loaded(super.getConf()) ?
      new DecompressorStream(in, decompressor,
              super.getConf().getInt("io.file.buffer.size", 4*1024)) :
      new CorrectedBZip2CompressionInputStream(in);
  }

  /**
   * Creates CompressionInputStream to be used to read off uncompressed data
   * in one of the two reading modes. i.e. Continuous or Blocked reading modes
   *
   * @param seekableIn The InputStream
   * @param start The start offset into the compressed stream
   * @param end The end offset into the compressed stream
   * @param readMode Controls whether progress is reported continuously or
   *                 only at block boundaries.
   *
   * @return CompressionInputStream for BZip2 aligned at block boundaries
   */
  public SplitCompressionInputStream createInputStream(InputStream seekableIn,
      Decompressor decompressor, long start, long end, READ_MODE readMode)
      throws IOException {

    if (!(seekableIn instanceof Seekable)) {
      throw new IOException("seekableIn must be an instance of " +
          Seekable.class.getName());
    }

    //find the position of first BZip2 start up marker
    ((Seekable)seekableIn).seek(0);

    // BZip2 start of block markers are of 6 bytes.  But the very first block
    // also has "BZh9", making it 10 bytes.  This is the common case.  But at
    // time stream might start without a leading BZ.
    final long FIRST_BZIP2_BLOCK_MARKER_POSITION =
      CorrectedCBZip2InputStream.numberOfBytesTillNextMarker(seekableIn);
    long adjStart = Math.max(0L, start - FIRST_BZIP2_BLOCK_MARKER_POSITION);

    ((Seekable)seekableIn).seek(adjStart);
    SplitCompressionInputStream in =
      new CorrectedBZip2CompressionInputStream(seekableIn, adjStart, end, readMode);


    // The following if clause handles the following case:
    // Assume the following scenario in BZip2 compressed stream where
    // . represent compressed data.
    // .....[48 bit Block].....[48 bit   Block].....[48 bit Block]...
    // ........................[47 bits][1 bit].....[48 bit Block]...
    // ................................^[Assume a Byte alignment here]
    // ........................................^^[current position of stream]
    // .....................^^[We go back 10 Bytes in stream and find a Block marker]
    // ........................................^^[We align at wrong position!]
    // ...........................................................^^[While this pos is correct]

    if (in.getPos() <= start) {
      ((Seekable)seekableIn).seek(start);
      in = new CorrectedBZip2CompressionInputStream(seekableIn, start, end, readMode);
    }

    return in;
  }

  /**
   * This class is capable to de-compress BZip2 data in two modes;
   * CONTINOUS and BYBLOCK.  BYBLOCK mode makes it possible to
   * do decompression starting any arbitrary position in the stream.
   *
   * So this facility can easily be used to parallelize decompression
   * of a large BZip2 file for performance reasons.  (It is exactly
   * done so for Hadoop framework.  See LineRecordReader for an
   * example).  So one can break the file (of course logically) into
   * chunks for parallel processing.  These "splits" should be like
   * default Hadoop splits (e.g as in FileInputFormat getSplit metod).
   * So this code is designed and tested for FileInputFormat's way
   * of splitting only.
   */

  private static class CorrectedBZip2CompressionInputStream extends
      SplitCompressionInputStream {

    // class data starts here//
    private CorrectedCBZip2InputStream input;
    boolean needsReset;
    private BufferedInputStream bufferedIn;
    private boolean isHeaderStripped = false;
    private boolean isSubHeaderStripped = false;
    private READ_MODE readMode = READ_MODE.CONTINUOUS;
    private long startingPos = 0L;

    // Following state machine handles different states of compressed stream
    // position
    // HOLD : Don't advertise compressed stream position
    // ADVERTISE : Read 1 more character and advertise stream position
    // See more comments about it before updatePos method.
    private enum POS_ADVERTISEMENT_STATE_MACHINE {
      HOLD, ADVERTISE
    };

    POS_ADVERTISEMENT_STATE_MACHINE posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
    long compressedStreamPosition = 0;

    // class data ends here//

    public CorrectedBZip2CompressionInputStream(InputStream in) throws IOException {
      this(in, 0L, Long.MAX_VALUE, READ_MODE.CONTINUOUS);
    }

    public CorrectedBZip2CompressionInputStream(InputStream in, long start, long end,
        READ_MODE readMode) throws IOException {
      super(in, start, end);
      needsReset = false;
      bufferedIn = new BufferedInputStream(super.in);
      this.startingPos = super.getPos();
      this.readMode = readMode;
      if (this.startingPos == 0) {
        // We only strip header if it is start of file
        bufferedIn = readStreamHeader();
      }
      input = new CorrectedCBZip2InputStream(bufferedIn, readMode);
      if (this.isHeaderStripped) {
        input.updateReportedByteCount(HEADER_LEN);
      }

      if (this.isSubHeaderStripped) {
        input.updateReportedByteCount(SUB_HEADER_LEN);
      }

      this.updatePos(false);
    }

    private BufferedInputStream readStreamHeader() throws IOException {
      // We are flexible enough to allow the compressed stream not to
      // start with the header of BZ. So it works fine either we have
      // the header or not.
      if (super.in != null) {
        bufferedIn.mark(HEADER_LEN);
        byte[] headerBytes = new byte[HEADER_LEN];
        int actualRead = bufferedIn.read(headerBytes, 0, HEADER_LEN);
        if (actualRead != -1) {
          String header = new String(headerBytes);
          if (header.compareTo(HEADER) != 0) {
            bufferedIn.reset();
          } else {
            this.isHeaderStripped = true;
            // In case of BYBLOCK mode, we also want to strip off
            // remaining two character of the header.
            if (this.readMode == READ_MODE.BYBLOCK) {
              actualRead = bufferedIn.read(headerBytes, 0,
                  SUB_HEADER_LEN);
              if (actualRead != -1) {
                this.isSubHeaderStripped = true;
              }
            }
          }
        }
      }

      if (bufferedIn == null) {
        throw new IOException("Failed to read bzip2 stream.");
      }

      return bufferedIn;

    }// end of method

    public void close() throws IOException {
      if (!needsReset) {
        input.close();
        needsReset = true;
      }
    }

    /**
    * This method updates compressed stream position exactly when the
    * client of this code has read off at least one byte passed any BZip2
    * end of block marker.
    *
    * This mechanism is very helpful to deal with data level record
    * boundaries. Please see constructor and next methods of
    * org.apache.hadoop.mapred.LineRecordReader as an example usage of this
    * feature.  We elaborate it with an example in the following:
    *
    * Assume two different scenarios of the BZip2 compressed stream, where
    * [m] represent end of block, \n is line delimiter and . represent compressed
    * data.
    *
    * ............[m]......\n.......
    *
    * ..........\n[m]......\n.......
    *
    * Assume that end is right after [m].  In the first case the reading
    * will stop at \n and there is no need to read one more line.  (To see the
    * reason of reading one more line in the next() method is explained in LineRecordReader.)
    * While in the second example LineRecordReader needs to read one more line
    * (till the second \n).  Now since BZip2Codecs only update position
    * at least one byte passed a maker, so it is straight forward to differentiate
    * between the two cases mentioned.
    *
    */

    public int read(byte[] b, int off, int len) throws IOException {
      if (needsReset) {
        internalReset();
      }

      int result = 0;
      result = this.input.read(b, off, len);
      if (result == BZip2Constants.END_OF_BLOCK) {
        this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE;
      }

      if (this.posSM == POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE) {
        result = this.input.read(b, off, off + 1);
        // This is the precise time to update compressed stream position
        // to the client of this code.
        this.updatePos(true);
        this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
      }

      return result;

    }

    public int read() throws IOException {
      byte b[] = new byte[1];
      int result = this.read(b, 0, 1);
      return (result < 0) ? result : (b[0] & 0xff);
    }

    private void internalReset() throws IOException {
      if (needsReset) {
        needsReset = false;
        BufferedInputStream bufferedIn = readStreamHeader();
        input = new CorrectedCBZip2InputStream(bufferedIn, this.readMode);
      }
    }    
    
    public void resetState() throws IOException {
      // Cannot read from bufferedIn at this point because bufferedIn
      // might not be ready
      // yet, as in SequenceFile.Reader implementation.
      needsReset = true;
    }

    public long getPos() {
      return this.compressedStreamPosition;
      }

    /*
     * As the comments before read method tell that
     * compressed stream is advertised when at least
     * one byte passed EOB have been read off.  But
     * there is an exception to this rule.  When we
     * construct the stream we advertise the position
     * exactly at EOB.  In the following method
     * shouldAddOn boolean captures this exception.
     *
     */
    private void updatePos(boolean shouldAddOn) {
      int addOn = shouldAddOn ? 1 : 0;
      this.compressedStreamPosition = this.startingPos
          + this.input.getProcessedByteCount() + addOn;
    }

  }// end of BZip2CompressionInputStream

}
