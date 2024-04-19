/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;

/**
 * Extended data input.
 */
public interface IgniteDataInput extends DataInput {
    /**
     * Sets the internal buffer.
     *
     * @param bytes Bytes.
     * @param len Length.
     */
    void bytes(byte[] bytes, int len);

    /**
     * Sets the {@link InputStream} from which to pull data.
     *
     * @param in Underlying input stream.
     * @throws IOException In case of error.
     */
    void inputStream(InputStream in) throws IOException;

    /**
     * Resets data input to the position remembered with {@link #mark(int)}.
     *
     * @throws IOException In case of error.
     */
    void reset() throws IOException;

    /**
     * Makes the data input ready for reuse.
     *
     * @throws IOException if something goes wrong
     */
    void cleanup() throws IOException;

    /**
     * Reads one byte and returns it or a negative value if the end of stream is reached.
     *
     * @return The next byte of data, or {@code -1} if the end of the stream is reached.
     * @exception IOException In case of error.
     */
    int read() throws IOException;

    /**
     * Reads data to fill (probably, partly) the given array.
     *
     * @param b Buffer into which the data is read.
     * @return Total number of bytes read into the buffer, or {@code -1} is there is no
     *     more data because the end of the stream has been reached.
     * @exception IOException In case of error.
     */
    int read(byte[] b) throws IOException;

    /**
     * Reads data to fill (probably, partly) the given array region.
     *
     * @param b Buffer into which the data is read.
     * @param off Start offset.
     * @param len Maximum number of bytes to read.
     * @return Total number of bytes read into the buffer, or {@code -1} is there is no
     *     more data because the end of the stream has been reached.
     * @exception IOException In case of error.
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Reads array of {@code byte}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    byte[] readByteArray(int length) throws IOException;

    /**
     * Reads array of {@code short}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    short[] readShortArray(int length) throws IOException;

    /**
     * Reads array of {@code int}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    int[] readIntArray(int length) throws IOException;

    /**
     * Reads array of {@code long}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    long[] readLongArray(int length) throws IOException;

    /**
     * Reads array of {@code float}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    float[] readFloatArray(int length) throws IOException;

    /**
     * Reads array of {@code double}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    double[] readDoubleArray(int length) throws IOException;

    /**
     * Reads array of {@code boolean}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    boolean[] readBooleanArray(int length) throws IOException;

    /**
     * Reads array of {@code char}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    char[] readCharArray(int length) throws IOException;

    /**
     * Reads big integer.
     *
     * @return Big integer.
     * @throws IOException In case of error.
     */
    BigInteger readBigInteger() throws IOException;

    /**
     * Reads big decimal.
     *
     * @return Big decimal.
     * @throws IOException In case of error.
     */
    BigDecimal readBigDecimal() throws IOException;

    /**
     * Reads local time.
     *
     * @return Local time.
     * @throws IOException In case of error.
     */
    LocalTime readLocalTime() throws IOException;

    /**
     * Reads local date.
     *
     * @return Local date.
     * @throws IOException In case of error.
     */
    LocalDate readLocalDate() throws IOException;

    /**
     * Reads local date time.
     *
     * @return Local date time.
     * @throws IOException In case of error.
     */
    LocalDateTime readLocalDateTime() throws IOException;

    /**
     * Reads instant.
     *
     * @return Instant.
     * @throws IOException In case of error.
     */
    Instant readInstant() throws IOException;

    /**
     * Reads duration.
     *
     * @return Duration.
     * @throws IOException In case of error.
     */
    Duration readDuration() throws IOException;

    /**
     * Reads period.
     *
     * @return Period.
     * @throws IOException In case of error.
     */
    Period readPeriod() throws IOException;

    /**
     * Reads uuid.
     *
     * @return UUID.
     * @throws IOException In case of error.
     */
    UUID readUuid() throws IOException;

    /**
     * Reads bit set.
     *
     * @return Bit set.
     * @throws IOException In case of error.
     */
    BitSet readBitSet() throws IOException;

    /**
     * Reads the requested number of bytes from the input stream into the given
     * byte array. This method blocks until {@code len} bytes of input data have
     * been read, end of stream is detected, or an exception is thrown. The
     * number of bytes actually read, possibly zero, is returned. This method
     * does not close the input stream.
     *
     * <p>In the case where end of stream is reached before {@code len} bytes
     * have been read, then the actual number of bytes read will be returned.
     * When this stream reaches end of stream, further invocations of this
     * method will return zero.
     *
     * <p>If {@code len} is zero, then no bytes are read and {@code 0} is
     * returned; otherwise, there is an attempt to read up to {@code len} bytes.
     *
     * <p>The first byte read is stored into element {@code b[off]}, the next
     * one in to {@code b[off+1]}, and so on. The number of bytes read is, at
     * most, equal to {@code len}. Let <i>k</i> be the number of bytes actually
     * read; these bytes will be stored in elements {@code b[off]} through
     * {@code b[off+}<i>k</i>{@code -1]}, leaving elements {@code b[off+}<i>k</i>
     * {@code ]} through {@code b[off+len-1]} unaffected.
     *
     * <p>The behavior for the case where the input stream is <i>asynchronously
     * closed</i>, or the thread interrupted during the read, is highly input
     * stream specific, and therefore not specified.
     *
     * <p>If an I/O error occurs reading from the input stream, then it may do
     * so after some, but not all, bytes of {@code b} have been updated with
     * data from the input stream. Consequently the input stream and {@code b}
     * may be in an inconsistent state. It is strongly recommended that the
     * stream be promptly closed if an I/O error occurs.
     *
     * @param  b the byte array into which the data is read
     * @param  off the start offset in {@code b} at which the data is written
     * @param  len the maximum number of bytes to read
     * @return the actual number of bytes read into the buffer
     * @throws IOException if an I/O error occurs
     * @throws NullPointerException if {@code b} is {@code null}
     * @throws IndexOutOfBoundsException If {@code off} is negative, {@code len}
     *     is negative, or {@code len} is greater than {@code b.length - off}
     */
    int readFewBytes(byte[] b, int off, int len) throws IOException;

    /**
     * Reads all remaining bytes from the input stream. This method blocks until
     * all remaining bytes have been read and end of stream is detected, or an
     * exception is thrown. This method does not close the input stream.
     *
     * <p>When this stream reaches end of stream, further invocations of this
     * method will return an empty byte array.
     *
     * <p>Note that this method is intended for simple cases where it is
     * convenient to read all bytes into a byte array. It is not intended for
     * reading input streams with large amounts of data.
     *
     * <p>The behavior for the case where the input stream is <i>asynchronously
     * closed</i>, or the thread interrupted during the read, is highly input
     * stream specific, and therefore not specified.
     *
     * <p>If an I/O error occurs reading from the input stream, then it may do
     * so after some, but not all, bytes have been read. Consequently the input
     * stream may not be at end of stream and may be in an inconsistent state.
     * It is strongly recommended that the stream be promptly closed if an I/O
     * error occurs.
     *
     * @return a byte array containing the bytes read from this input stream
     * @throws IOException if an I/O error occurs
     * @throws OutOfMemoryError if an array of the required size cannot be
     *     allocated.
     */
    byte[] readAllBytes() throws IOException;

    /**
     * Marks the current position in this input stream. A subsequent call to
     * the <code>reset</code> method repositions this stream at the last marked
     * position so that subsequent reads re-read the same bytes.
     *
     * <p>The <code>readlimit</code> arguments tells this input stream to
     * allow that many bytes to be read before the mark position gets
     * invalidated.
     *
     * <p>The general contract of <code>mark</code> is that, if the method
     * <code>markSupported</code> returns <code>true</code>, the stream somehow
     * remembers all the bytes read after the call to <code>mark</code> and
     * stands ready to supply those same bytes again if and whenever the method
     * <code>reset</code> is called.  However, the stream is not required to
     * remember any data at all if more than <code>readlimit</code> bytes are
     * read from the stream before <code>reset</code> is called.
     *
     * <p>Marking a closed stream should not have any effect on the stream.
     *
     * <p>The <code>mark</code> method of <code>InputStream</code> does
     * nothing.
     *
     * @param   readLimit   the maximum limit of bytes that can be read before
     *                      the mark position becomes invalid.
     * @see     java.io.InputStream#reset()
     */
    void mark(int readLimit);

    /**
     * Returns an estimate of the number of bytes that can be read (or skipped
     * over) from this input stream without blocking, which may be 0, or 0 when
     * end of stream is detected.  The read might be on the same thread or
     * another thread.  A single read or skip of this many bytes will not block,
     * but may read or skip fewer bytes.
     *
     * <p>Note that while some implementations of {@code InputStream} will
     * return the total number of bytes in the stream, many will not.  It is
     * never correct to use the return value of this method to allocate
     * a buffer intended to hold all data in this stream.
     *
     * <p>A subclass's implementation of this method may choose to throw an
     * {@link IOException} if this input stream has been closed by invoking the
     * {@link #close()} method.
     *
     * <p>The {@code available} method of {@code InputStream} always returns
     * {@code 0}.
     *
     * <p>This method should be overridden by subclasses.
     *
     * @return     an estimate of the number of bytes that can be read (or
     *     skipped over) from this input stream without blocking or
     *     {@code 0} when it reaches the end of the input stream.
     * @exception  IOException if an I/O error occurs.
     */
    int available() throws IOException;

    /**
     * Materializes an object from next bytes (the count is known upfront). The bytes used for materialization are consumed.
     * This method is useful to avoid an allocation when first reading bytes to a byte array and then converting them
     * to an object.
     *
     * @param bytesCount   number of bytes to consume and use for materialization
     * @param materializer materializer to turn the bytes to an object
     * @param <T>          materialized object type
     * @return the materialized object
     * @throws IOException if an I/O error occurs
     */
    <T> T materializeFromNextBytes(int bytesCount, Materializer<? extends T> materializer) throws IOException;

    /**
     * Materializer used to turn a region of a byte array to an object.
     *
     * @param <T> the type of a materialized object
     */
    interface Materializer<T> {
        /**
         * Materializes an object from a region of the given byte array.
         *
         * @param buffer byte array
         * @param offset offset to that array where the bytes representing the object to be materialized start
         * @param length length (in bytes) of the region to use for materialization
         * @return the materialized object
         */
        T materialize(byte[] buffer, int offset, int length);
    }
}
