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

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.CHAR_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.DOUBLE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.FLOAT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.INT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.IS_BIG_ENDIAN;
import static org.apache.ignite.internal.util.GridUnsafe.LONG_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.SHORT_ARR_OFF;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Data input based on {@code Unsafe} operations.
 */
public class IgniteUnsafeDataInput extends InputStream implements IgniteDataInput {
    /** Maximum data block length. */
    private static final int MAX_BLOCK_SIZE = 1024;

    /** Length of char buffer (for reading strings). */
    private static final int CHAR_BUF_SIZE = 256;

    private static final long NO_AUTO_SHRINK = -1;

    private final long shrinkCheckFrequencyMs;

    /** Buffer for reading general/block data. */
    @IgniteToStringExclude
    private final byte[] utfBuf = new byte[MAX_BLOCK_SIZE];

    /** Char buffer for fast string reads. */
    @IgniteToStringExclude
    private final char[] utfCharBuf = new char[CHAR_BUF_SIZE];

    /** Current offset into buf. */
    private int pos;

    /** End offset of valid data in buf, or -1 if no more block data. */
    private int end = -1;

    /** Bytes. */
    @IgniteToStringExclude
    private byte[] buf;

    /** Offset. */
    private int off;

    /** Max. */
    private int max;

    /** Underlying input stream. */
    @IgniteToStringExclude
    private InputStream in;

    /** Buffer for reading from stream. */
    @IgniteToStringExclude
    private byte[] inBuf = new byte[1024];

    /** Maximum message size. */
    private int maxOff;

    private int mark;

    /** Last length check timestamp. */
    private long lastAutoShrinkCheckTimestamp;

    /**
     * Creates a new input with auto-shrinking disabled and no internal buffer assigned.
     */
    public IgniteUnsafeDataInput() {
        this(NO_AUTO_SHRINK);
    }

    /**
     * Creates a new input with no internal buffer assigned.
     *
     * @param shrinkCheckFrequencyMs how often to check whether an underlying byte buffer needs to be shrunk
     *                               (disables the auto-shrinking if it's -1)
     */
    public IgniteUnsafeDataInput(long shrinkCheckFrequencyMs) {
        this.shrinkCheckFrequencyMs = shrinkCheckFrequencyMs;
    }

    /**
     * Creates a new input with auto-shrinking disabled.
     *
     * @param bytes array to initially (before automatic resize) use as an internal buffer
     */
    public IgniteUnsafeDataInput(byte[] bytes) {
        this(bytes, NO_AUTO_SHRINK);
    }

    /**
     * Creates a new input.
     *
     * @param bytes array to initially (before automatic resize) use as an internal buffer
     * @param shrinkCheckFrequencyMs how often to check whether an underlying byte buffer needs to be shrunk
     *                               (disables the auto-shrinking if it's -1)
     */
    public IgniteUnsafeDataInput(byte[] bytes, long shrinkCheckFrequencyMs) {
        this(shrinkCheckFrequencyMs);

        bytes(bytes, bytes.length);
    }

    /** {@inheritDoc} */
    @Override
    public void bytes(byte[] bytes, int len) {
        bytes(bytes, 0, len);
    }

    /**
     * Sets the internal buffer.
     *
     * @param bytes Bytes.
     * @param off Offset.
     * @param len Length.
     */
    public void bytes(byte[] bytes, int off, int len) {
        buf = bytes;

        max = len;
        this.off = off;
    }

    /** {@inheritDoc} */
    @Override
    public void inputStream(InputStream in) throws IOException {
        this.in = in;

        buf = inBuf;
    }

    /**
     * Reads from stream to buffer. If stream is {@code null}, this method is no-op.
     *
     * @param size Number of bytes to read.
     * @throws IOException In case of error.
     */
    private void fromStream(int size) throws IOException {
        if (in == null) {
            return;
        }

        maxOff = Math.max(maxOff, size);

        // Increase size of buffer if needed.
        if (size > inBuf.length) {
            buf = inBuf = new byte[Math.max(inBuf.length << 1, size)]; // Grow.
        } else if (isAutoShrinkEnabled()) {
            long now = FastTimestamps.coarseCurrentTimeMillis();

            if (now - lastAutoShrinkCheckTimestamp > shrinkCheckFrequencyMs) {
                int halfSize = inBuf.length >> 1;

                if (maxOff < halfSize) {
                    byte[] newInBuf = new byte[halfSize]; // Shrink.

                    System.arraycopy(inBuf, 0, newInBuf, 0, off);

                    buf = inBuf = newInBuf;
                }

                maxOff = 0;
                lastAutoShrinkCheckTimestamp = now;
            }
        }

        off = 0;
        max = 0;

        while (max != size) {
            int read = in.read(inBuf, max, size - max);

            if (read == -1) {
                throw new EOFException("End of stream reached: " + in);
            }

            max += read;
        }
    }

    private boolean isAutoShrinkEnabled() {
        return shrinkCheckFrequencyMs != NO_AUTO_SHRINK;
    }

    /**
     * Advances the offset doing a check for whether we fell off the buffer edge.
     *
     * @param more Bytes to move forward.
     * @return Old offset value.
     * @throws IOException In case of error.
     */
    private int advanceOffset(int more) throws IOException {
        int old = off;

        off += more;

        if (off > max) {
            throw new EOFException("Attempt to read beyond the end of the stream "
                    + "[pos=" + off + ", more=" + more + ", max=" + max + ']');
        }

        return old;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override
    public void reset() throws IOException {
        if (in != null) {
            in.reset();
        } else {
            off = mark;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void cleanup() throws IOException {
        in = null;

        off = 0;
        max = 0;
        mark = 0;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] readByteArray(int arrSize) throws IOException {
        fromStream(arrSize);

        byte[] arr = new byte[arrSize];

        System.arraycopy(buf, advanceOffset(arrSize), arr, 0, arrSize);

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public short[] readShortArray(int arrSize) throws IOException {
        int bytesToCp = arrSize << 1;

        fromStream(bytesToCp);

        short[] arr = new short[arrSize];

        long off = BYTE_ARR_OFF + advanceOffset(bytesToCp);

        if (IS_BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getShortLittleEndian(buf, off);

                off += 2;
            }
        } else {
            GridUnsafe.copyMemory(buf, off, arr, SHORT_ARR_OFF, bytesToCp);
        }

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public int[] readIntArray(int arrSize) throws IOException {
        int bytesToCp = arrSize << 2;

        fromStream(bytesToCp);

        int[] arr = new int[arrSize];

        long off = BYTE_ARR_OFF + advanceOffset(bytesToCp);

        if (IS_BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getIntLittleEndian(buf, off);

                off += 4;
            }
        } else {
            GridUnsafe.copyMemory(buf, off, arr, INT_ARR_OFF, bytesToCp);
        }

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public double[] readDoubleArray(int arrSize) throws IOException {
        int bytesToCp = arrSize << 3;

        fromStream(bytesToCp);

        double[] arr = new double[arrSize];

        long off = BYTE_ARR_OFF + advanceOffset(bytesToCp);

        if (IS_BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getDoubleLittleEndian(buf, off);

                off += 8;
            }
        } else {
            GridUnsafe.copyMemory(buf, off, arr, DOUBLE_ARR_OFF, bytesToCp);
        }

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public boolean[] readBooleanArray(int arrSize) throws IOException {
        boolean[] vals = new boolean[arrSize];

        for (int i = 0; i < arrSize; i++) {
            vals[i] = readBoolean();
        }

        return vals;
    }

    /** {@inheritDoc} */
    @Override
    public char[] readCharArray(int arrSize) throws IOException {
        int bytesToCp = arrSize << 1;

        fromStream(bytesToCp);

        char[] arr = new char[arrSize];

        long off = BYTE_ARR_OFF + advanceOffset(bytesToCp);

        if (IS_BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getCharLittleEndian(buf, off);

                off += 2;
            }
        } else {
            GridUnsafe.copyMemory(buf, off, arr, CHAR_ARR_OFF, bytesToCp);
        }

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public BigInteger readBigInteger() throws IOException {
        int length = readInt();
        byte[] bytes = readByteArray(length);
        return new BigInteger(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal readBigDecimal() throws IOException {
        short scale = readShort();
        BigInteger bigInteger = readBigInteger();

        return new BigDecimal(bigInteger, scale);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime readLocalTime() throws IOException {
        byte hour = readByte();
        byte minute = readByte();
        byte second = readByte();
        int nano = readInt();

        return LocalTime.of(hour, minute, second, nano);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate readLocalDate() throws IOException {
        int year = readInt();
        short month = readShort();
        short day = readShort();

        return LocalDate.of(year, month, day);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime readLocalDateTime() throws IOException {
        int year = readInt();
        short month = readShort();
        short day = readShort();
        byte hour = readByte();
        byte minute = readByte();
        byte second = readByte();
        int nano = readInt();

        return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }

    /** {@inheritDoc} */
    @Override
    public Instant readInstant() throws IOException {
        long epochSecond = readLong();
        int nano = readInt();
        return Instant.ofEpochSecond(epochSecond, nano);
    }


    /** {@inheritDoc} */
    @Override
    public Duration readDuration() throws IOException {
        long seconds = readLong();
        int nano = readInt();

        return Duration.ofSeconds(seconds, nano);
    }

    /** {@inheritDoc} */
    @Override
    public Period readPeriod() throws IOException {
        int years = readInt();
        int months = readInt();
        int days = readInt();

        return Period.of(years, months, days);
    }

    /** {@inheritDoc} */
    @Override
    public UUID readUuid() throws IOException {
        int length = readByte();
        byte[] bytes = readByteArray(length);

        return UUID.fromString(new String(bytes, StandardCharsets.UTF_8));
    }

    /** {@inheritDoc} */
    @Override
    public BitSet readBitSet() throws IOException {
        int length = readInt();
        byte[] bytes = readByteArray(length);

        return BitSet.valueOf(bytes);
    }


    /** {@inheritDoc} */
    @Override
    public long[] readLongArray(int arrSize) throws IOException {
        int bytesToCp = arrSize << 3;

        fromStream(bytesToCp);

        long[] arr = new long[arrSize];

        long off = BYTE_ARR_OFF + advanceOffset(bytesToCp);

        if (IS_BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getLongLittleEndian(buf, off);

                off += 8;
            }
        } else {
            GridUnsafe.copyMemory(buf, off, arr, LONG_ARR_OFF, bytesToCp);
        }

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public float[] readFloatArray(int arrSize) throws IOException {
        int bytesToCp = arrSize << 2;

        fromStream(bytesToCp);

        float[] arr = new float[arrSize];

        long off = BYTE_ARR_OFF + advanceOffset(bytesToCp);

        if (IS_BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getFloatLittleEndian(buf, off);

                off += 4;
            }
        } else {
            GridUnsafe.copyMemory(buf, off, arr, FLOAT_ARR_OFF, bytesToCp);
        }

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public int readFewBytes(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);

        int n = 0;
        while (n < len) {
            int count = read(b, off + n, len - n);
            if (count < 0) {
                break;
            }
            n += count;
        }
        return n;
    }

    /** {@inheritDoc} */
    @Override
    public void readFully(byte[] b) throws IOException {
        int len = b.length;

        fromStream(len);

        System.arraycopy(buf, advanceOffset(len), b, 0, len);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        fromStream(len);

        System.arraycopy(buf, advanceOffset(len), b, off, len);
    }

    /** {@inheritDoc} */
    @Override
    public int skipBytes(int n) {
        if (off + n > max) {
            n = max - off;
        }

        off += n;

        return n;
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean() throws IOException {
        fromStream(1);

        return GridUnsafe.getBoolean(buf, BYTE_ARR_OFF + advanceOffset(1));
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() throws IOException {
        fromStream(1);

        return GridUnsafe.getByte(buf, BYTE_ARR_OFF + advanceOffset(1));
    }

    /** {@inheritDoc} */
    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() throws IOException {
        fromStream(2);

        long off = BYTE_ARR_OFF + advanceOffset(2);

        return IS_BIG_ENDIAN ? GridUnsafe.getShortLittleEndian(buf, off) : GridUnsafe.getShort(buf, off);
    }

    /** {@inheritDoc} */
    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    /** {@inheritDoc} */
    @Override
    public char readChar() throws IOException {
        fromStream(2);

        long off = BYTE_ARR_OFF + this.off;

        char v = IS_BIG_ENDIAN ? GridUnsafe.getCharLittleEndian(buf, off) : GridUnsafe.getChar(buf, off);

        advanceOffset(2);

        return v;
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() throws IOException {
        fromStream(4);

        long off = BYTE_ARR_OFF + advanceOffset(4);

        return IS_BIG_ENDIAN ? GridUnsafe.getIntLittleEndian(buf, off) : GridUnsafe.getInt(buf, off);
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() throws IOException {
        fromStream(8);

        long off = BYTE_ARR_OFF + advanceOffset(8);

        return IS_BIG_ENDIAN ? GridUnsafe.getLongLittleEndian(buf, off) : GridUnsafe.getLong(buf, off);
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() throws IOException {
        int v = readInt();

        return Float.intBitsToFloat(v);
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() throws IOException {
        long v = readLong();

        return Double.longBitsToDouble(v);
    }

    /** {@inheritDoc} */
    @Override
    public int read() throws IOException {
        try {
            return readUnsignedByte();
        } catch (EOFException ignored) {
            return -1;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (len == 0) {
            return 0;
        }

        if (in != null) {
            return in.read(b, off, len);
        } else {
            int toRead = Math.min(len, max - this.off);
            if (toRead <= 0) {
                return -1;
            }

            System.arraycopy(buf, advanceOffset(toRead), b, off, toRead);

            return toRead;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();

        int b;

        while ((b = read()) >= 0) {
            char c = (char) b;

            switch (c) {
                case '\n':
                    return sb.toString();

                case '\r':
                    b = read();

                    if (b < 0 || b == '\n') {
                        return sb.toString();
                    } else {
                        sb.append((char) b);
                    }

                    break;

                default:
                    sb.append(c);
            }
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override
    public String readUTF() throws IOException {
        return readUtfBody(VarInts.readUnsignedInt(this));
    }

    /**
     * Reads in the "body" (i.e., the UTF representation minus the 2-byte
     * or 8-byte length header) of a UTF encoding, which occupies the next
     * utfLen bytes.
     *
     * @param utfLen UTF encoding length.
     * @return String.
     * @throws IOException In case of error.
     */
    private String readUtfBody(long utfLen) throws IOException {
        StringBuilder sbuf = new StringBuilder();

        end = pos = 0;

        while (utfLen > 0) {
            int avail = end - pos;

            if (avail >= 3 || (long) avail == utfLen) {
                utfLen -= readUtfSpan(sbuf, utfLen);
            } else {
                // shift and refill buffer manually
                if (avail > 0) {
                    System.arraycopy(utfBuf, pos, utfBuf, 0, avail);
                }

                pos = 0;
                end = (int) Math.min(MAX_BLOCK_SIZE, utfLen);

                readFully(utfBuf, avail, end - avail);
            }
        }

        return sbuf.toString();
    }

    /**
     * Reads span of UTF-encoded characters out of internal buffer
     * (starting at offset pos and ending at or before offset end),
     * consuming no more than utfLen bytes. Appends read characters to
     * sbuf. Returns the number of bytes consumed.
     *
     * @param sbuf String builder.
     * @param utfLen UTF encoding length.
     * @return Number of bytes consumed.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private long readUtfSpan(StringBuilder sbuf, long utfLen) throws IOException {
        int cpos = 0;
        int start = pos;
        int avail = Math.min(end - pos, CHAR_BUF_SIZE);
        int stop = pos + ((utfLen > avail) ? avail - 2 : (int) utfLen);
        boolean outOfBounds = false;

        try {
            while (pos < stop) {
                int b1 = utfBuf[pos++] & 0xFF;

                int b2;
                int b3;

                switch (b1 >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        // 1 byte format: 0xxxxxxx
                        utfCharBuf[cpos++] = (char) b1;

                        break;

                    case 12:
                    case 13:
                        // 2 byte format: 110xxxxx 10xxxxxx
                        b2 = utfBuf[pos++];

                        if ((b2 & 0xC0) != 0x80) {
                            throw new UTFDataFormatException();
                        }

                        utfCharBuf[cpos++] = (char) (((b1 & 0x1F) << 6) | (b2 & 0x3F));

                        break;

                    case 14:
                        // 3 byte format: 1110xxxx 10xxxxxx 10xxxxxx
                        b3 = utfBuf[pos + 1];
                        b2 = utfBuf[pos];

                        pos += 2;

                        if ((b2 & 0xC0) != 0x80 || (b3 & 0xC0) != 0x80) {
                            throw new UTFDataFormatException();
                        }

                        utfCharBuf[cpos++] = (char) (((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));

                        break;

                    default:
                        // 10xx xxxx, 1111 xxxx
                        throw new UTFDataFormatException();
                }
            }
        } catch (ArrayIndexOutOfBoundsException ignored) {
            outOfBounds = true;
        } finally {
            if (outOfBounds || (pos - start) > utfLen) {
                pos = start + (int) utfLen;

                throw new UTFDataFormatException();
            }
        }

        sbuf.append(utfCharBuf, 0, cpos);

        return pos - start;
    }

    /** {@inheritDoc} */
    @Override
    public void mark(int readLimit) {
        if (in != null) {
            in.mark(readLimit);
        } else {
            mark = off;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean markSupported() {
        return in == null || in.markSupported();
    }

    /** {@inheritDoc} */
    @Override
    public int available() throws IOException {
        if (in != null) {
            return in.available();
        } else {
            return Math.max(buf.length - off, 0);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> T materializeFromNextBytes(int bytesCount, Materializer<? extends T> materializer) throws IOException {
        fromStream(bytesCount);

        int prevOffset = advanceOffset(bytesCount);

        return materializer.materialize(buf, prevOffset, bytesCount);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return IgniteToStringBuilder.toString(IgniteUnsafeDataInput.class, this);
    }
}
