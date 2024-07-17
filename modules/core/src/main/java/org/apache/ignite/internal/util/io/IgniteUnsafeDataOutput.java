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

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.StringIntrospection;

/**
 * Data output based on {@code Unsafe} operations.
 */
public class IgniteUnsafeDataOutput extends OutputStream implements IgniteDataOutput {
    /**
     * Based on ByteArrayOutputStream#MAX_ARRAY_SIZE or many other similar constants in other classes.
     * It's not safe to allocate more then this number of elements in byte array, because it can throw
     * java.lang.OutOfMemoryError: Requested array size exceeds VM limit
     */
    private static final int MAX_BYTE_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /** Length of char buffer (for writing strings). */
    private static final int CHAR_BUF_SIZE = 256;

    /** Char buffer for fast string writes. */
    private final char[] cbuf = new char[CHAR_BUF_SIZE];

    private static final long NO_AUTO_SHRINK = -1;

    private final long shrinkCheckFrequencyMs;

    /** Bytes. */
    private byte[] bytes;

    /** Offset. */
    private int off;

    /** Underlying output stream. */
    private OutputStream out;

    /** Maximum message size. */
    private int maxOff;

    /** Last length check timestamp. */
    private long lastAutoShrinkCheckTimestamp;

    /**
     * Creates a new output with auto-shrinking disabled and no internal buffer allocated.
     */
    public IgniteUnsafeDataOutput() {
        this(NO_AUTO_SHRINK);
    }

    /**
     * Creates a new output with no internal buffer allocated.
     *
     * @param shrinkCheckFrequencyMs how often to check whether an underlying byte buffer needs to be shrunk
     *                               (disables the auto-shrinking if it's -1)
     */
    public IgniteUnsafeDataOutput(long shrinkCheckFrequencyMs) {
        if (shrinkCheckFrequencyMs != NO_AUTO_SHRINK && shrinkCheckFrequencyMs <= 0) {
            throw new IllegalArgumentException(
                    "Auto-shrink frequency must be either -1 (when auto-shrinking is disabled) or positive, but it's "
                            + shrinkCheckFrequencyMs
            );
        }

        this.shrinkCheckFrequencyMs = shrinkCheckFrequencyMs;
    }

    /**
     * Creates a new output with auto-shrinking disabled.
     *
     * @param size size of an internal buffer to create
     */
    public IgniteUnsafeDataOutput(int size) {
        this(size, NO_AUTO_SHRINK);
    }

    /**
     * Creates a new output.
     *
     * @param size Size of an internal buffer to create
     * @param shrinkCheckFrequencyMs how often to check whether an underlying byte buffer needs to be shrunk
     *                               (disables the auto-shrinking if it's -1)
     */
    public IgniteUnsafeDataOutput(int size, long shrinkCheckFrequencyMs) {
        this(shrinkCheckFrequencyMs);

        bytes = new byte[size];
    }

    /**
     * Sets the internal buffer.
     *
     * @param bytes Bytes.
     * @param off Offset.
     */
    public void bytes(byte[] bytes, int off) {
        this.bytes = bytes;
        this.off = off;
    }

    /**
     * Sets the {@link OutputStream} to which to push data.
     *
     * @param out Underlying output stream.
     */
    @Override
    public void outputStream(OutputStream out) {
        this.out = out;

        off = 0;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] array() {
        byte[] bytes0 = new byte[off];

        System.arraycopy(bytes, 0, bytes0, 0, off);

        return bytes0;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] internalArray() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override
    public int offset() {
        return off;
    }

    /** {@inheritDoc} */
    @Override
    public void offset(int off) {
        this.off = off;
    }

    private void requestFreeSize(int size) throws IOException {
        if (!canBeAllocated(off + size)) {
            throw new IOException("Failed to allocate required memory (byte array size overflow detected) "
                    + "[length=" + size + ", offset=" + off + ']');
        }

        size = off + size;

        maxOff = Math.max(maxOff, size);

        if (size > bytes.length) {
            int newSize = size << 1;

            if (!canBeAllocated(newSize)) {
                newSize = MAX_BYTE_ARRAY_SIZE;
            }

            bytes = Arrays.copyOf(bytes, newSize); // Grow.
        } else if (isAutoShrinkEnabled()) {
            long now = FastTimestamps.coarseCurrentTimeMillis();

            if (now - lastAutoShrinkCheckTimestamp > shrinkCheckFrequencyMs) {
                int halfSize = bytes.length >> 1;

                if (maxOff < halfSize) {
                    bytes = Arrays.copyOf(bytes, halfSize); // Shrink.
                }

                maxOff = 0;
                lastAutoShrinkCheckTimestamp = now;
            }
        }
    }

    private boolean isAutoShrinkEnabled() {
        return shrinkCheckFrequencyMs != NO_AUTO_SHRINK;
    }

    /**
     * Returns true if {@code new byte[size]} won't throw {@link OutOfMemoryError} given enough heap space.
     *
     * @param size Size of potential byte array to check.
     * @return true if {@code new byte[size]} won't throw {@link OutOfMemoryError} given enough heap space.
     * @see IgniteUnsafeDataOutput#MAX_BYTE_ARRAY_SIZE
     */
    private boolean canBeAllocated(long size) {
        return 0 <= size && size <= MAX_BYTE_ARRAY_SIZE;
    }

    /**
     * If there is an {@link OutputStream}, writes the requested amount of buffered data to it; otherwise,
     * just advances the offset of the internal buffer.
     *
     * @param size Size.
     * @throws IOException In case of error.
     */
    private void onWrite(int size) throws IOException {
        if (out != null) {
            out.write(bytes, 0, size);
        } else {
            off += size;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void write(int b) throws IOException {
        writeByte(b);
    }

    /** {@inheritDoc} */
    @Override
    public void write(byte[] b) throws IOException {
        requestFreeSize(b.length);

        System.arraycopy(b, 0, bytes, off, b.length);

        onWrite(b.length);
    }

    /** {@inheritDoc} */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        requestFreeSize(len);

        System.arraycopy(b, off, bytes, this.off, len);

        onWrite(len);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDoubleArray(double[] arr) throws IOException {
        checkArrayAllocationOverflow(8, arr.length, "double");

        int bytesToCp = arr.length << 3;

        requestFreeSize(bytesToCp);

        if (IS_BIG_ENDIAN) {
            long off = BYTE_ARR_OFF + this.off;

            for (double val : arr) {
                GridUnsafe.putDoubleLittleEndian(bytes, off, val);

                off += 8;
            }
        } else {
            GridUnsafe.copyMemory(arr, DOUBLE_ARR_OFF, bytes, BYTE_ARR_OFF + off, bytesToCp);
        }

        onWrite(bytesToCp);
    }

    private void putInt(int val, long off) {
        if (IS_BIG_ENDIAN) {
            GridUnsafe.putIntLittleEndian(bytes, off, val);
        } else {
            GridUnsafe.putInt(bytes, off, val);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override
    public void writeBooleanArray(boolean[] arr) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            writeBoolean(arr[i]);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeCharArray(char[] arr) throws IOException {
        checkArrayAllocationOverflow(2, arr.length, "char");

        int bytesToCp = arr.length << 1;

        requestFreeSize(bytesToCp);

        if (IS_BIG_ENDIAN) {
            long off = BYTE_ARR_OFF + this.off;

            for (char val : arr) {
                GridUnsafe.putCharLittleEndian(bytes, off, val);

                off += 2;
            }
        } else {
            GridUnsafe.copyMemory(arr, CHAR_ARR_OFF, bytes, BYTE_ARR_OFF + off, bytesToCp);
        }

        onWrite(bytesToCp);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBigInteger(BigInteger val) throws IOException {
        byte[] bytes = val.toByteArray();
        writeInt(bytes.length);
        writeByteArray(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBigDecimal(BigDecimal val) throws IOException {
        short scale = (short) val.scale();
        byte[] bytes = val.unscaledValue().toByteArray();

        writeShort(scale);
        writeInt(bytes.length);
        writeByteArray(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLocalTime(LocalTime val) throws IOException {
        byte hour = (byte) val.getHour();
        byte minute = (byte) val.getMinute();
        byte second = (byte) val.getSecond();
        int nano = val.getNano();

        writeByte(hour);
        writeByte(minute);
        writeByte(second);
        writeInt(nano);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLocalDate(LocalDate date) throws IOException {
        int year = date.getYear();
        short month = (short) date.getMonth().getValue();
        short day = (short) date.getDayOfMonth();

        writeInt(year);
        writeShort(month);
        writeShort(day);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLocalDateTime(LocalDateTime val) throws IOException {
        int year = val.getYear();
        short month = (short) val.getMonth().getValue();
        short day = (short) val.getDayOfMonth();

        byte hour = (byte) val.getHour();
        byte minute = (byte) val.getMinute();
        byte second = (byte) val.getSecond();
        int nano = val.getNano();

        writeInt(year);
        writeShort(month);
        writeShort(day);

        writeByte(hour);
        writeByte(minute);
        writeByte(second);
        writeInt(nano);
    }

    /** {@inheritDoc} */
    @Override
    public void writeInstant(Instant val) throws IOException {
        long epochSecond = val.getEpochSecond();
        int nano = val.getNano();

        writeLong(epochSecond);
        writeInt(nano);
    }

    /** {@inheritDoc} */
    @Override
    public void writePeriod(Period val) throws IOException {
        writeInt(val.getYears());
        writeInt(val.getMonths());
        writeInt(val.getDays());
    }

    /** {@inheritDoc} */
    @Override
    public void writeDuration(Duration val) throws IOException {
        writeLong(val.getSeconds());
        writeInt(val.getNano());
    }

    /** {@inheritDoc} */
    @Override
    public void writeUuid(UUID val) throws IOException {
        byte[] bytes = val.toString().getBytes(StandardCharsets.US_ASCII);

        writeByte(bytes.length);
        writeByteArray(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBitSet(BitSet val) throws IOException {
        byte[] bytes = val.toByteArray();

        writeInt(bytes.length);
        writeByteArray(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLongArray(long[] arr) throws IOException {
        checkArrayAllocationOverflow(8, arr.length, "long");

        int bytesToCp = arr.length << 3;

        requestFreeSize(bytesToCp);

        if (IS_BIG_ENDIAN) {
            long off = BYTE_ARR_OFF + this.off;

            for (long val : arr) {
                GridUnsafe.putLongLittleEndian(bytes, off, val);

                off += 8;
            }
        } else {
            GridUnsafe.copyMemory(arr, LONG_ARR_OFF, bytes, BYTE_ARR_OFF + off, bytesToCp);
        }

        onWrite(bytesToCp);
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloatArray(float[] arr) throws IOException {
        checkArrayAllocationOverflow(4, arr.length, "float");

        int bytesToCp = arr.length << 2;

        requestFreeSize(bytesToCp);

        if (IS_BIG_ENDIAN) {
            long off = BYTE_ARR_OFF + this.off;

            for (float val : arr) {
                GridUnsafe.putFloatLittleEndian(bytes, off, val);

                off += 4;
            }
        } else {
            GridUnsafe.copyMemory(arr, FLOAT_ARR_OFF, bytes, BYTE_ARR_OFF + off, bytesToCp);
        }

        onWrite(bytesToCp);
    }

    /** {@inheritDoc} */
    @Override
    public void cleanup() {
        off = 0;

        out = null;
    }

    /** {@inheritDoc} */
    @Override
    public void writeByteArray(byte[] arr) throws IOException {
        requestFreeSize(arr.length);

        System.arraycopy(arr, 0, bytes, off, arr.length);

        onWrite(arr.length);
    }

    /** {@inheritDoc} */
    @Override
    public void writeShortArray(short[] arr) throws IOException {
        checkArrayAllocationOverflow(2, arr.length, "short");

        int bytesToCp = arr.length << 1;

        requestFreeSize(bytesToCp);

        if (IS_BIG_ENDIAN) {
            long off = BYTE_ARR_OFF + this.off;

            for (short val : arr) {
                GridUnsafe.putShortLittleEndian(bytes, off, val);

                off += 2;
            }
        } else {
            GridUnsafe.copyMemory(arr, SHORT_ARR_OFF, bytes, BYTE_ARR_OFF + off, bytesToCp);
        }

        onWrite(bytesToCp);
    }

    /** {@inheritDoc} */
    @Override
    public void writeIntArray(int[] arr) throws IOException {
        checkArrayAllocationOverflow(4, arr.length, "int");

        int bytesToCp = arr.length << 2;

        requestFreeSize(bytesToCp);

        if (IS_BIG_ENDIAN) {
            long off = BYTE_ARR_OFF + this.off;

            for (int val : arr) {
                GridUnsafe.putIntLittleEndian(bytes, off, val);

                off += 4;
            }
        } else {
            GridUnsafe.copyMemory(arr, INT_ARR_OFF, bytes, BYTE_ARR_OFF + off, bytesToCp);
        }

        onWrite(bytesToCp);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        cleanup();
    }

    /** {@inheritDoc} */
    @Override
    public void writeBoolean(boolean v) throws IOException {
        requestFreeSize(1);

        GridUnsafe.putBoolean(bytes, BYTE_ARR_OFF + off, v);

        onWrite(1);
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(int v) throws IOException {
        requestFreeSize(1);

        putByte((byte) v, BYTE_ARR_OFF + off);

        onWrite(1);
    }

    private void putByte(byte val, long off) {
        GridUnsafe.putByte(bytes, off, val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(int v) throws IOException {
        requestFreeSize(2);

        short val = (short) v;

        long off = BYTE_ARR_OFF + this.off;

        putShort(val, off);

        onWrite(2);
    }

    private void putShort(short val, long off) {
        if (IS_BIG_ENDIAN) {
            GridUnsafe.putShortLittleEndian(bytes, off, val);
        } else {
            GridUnsafe.putShort(bytes, off, val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeChar(int v) throws IOException {
        requestFreeSize(2);

        char val = (char) v;

        long off = BYTE_ARR_OFF + this.off;

        if (IS_BIG_ENDIAN) {
            GridUnsafe.putCharLittleEndian(bytes, off, val);
        } else {
            GridUnsafe.putChar(bytes, off, val);
        }

        onWrite(2);
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int v) throws IOException {
        requestFreeSize(4);

        long off = BYTE_ARR_OFF + this.off;

        putInt(v, off);

        onWrite(4);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long v) throws IOException {
        requestFreeSize(8);

        long off = BYTE_ARR_OFF + this.off;

        if (IS_BIG_ENDIAN) {
            GridUnsafe.putLongLittleEndian(bytes, off, v);
        } else {
            GridUnsafe.putLong(bytes, off, v);
        }

        onWrite(8);
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float v) throws IOException {
        int val = Float.floatToIntBits(v);

        writeInt(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double v) throws IOException {
        long val = Double.doubleToLongBits(v);

        writeLong(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBytes(String s) throws IOException {
        int len = s.length();

        if (utfLength(s) == len) {
            writeAsciiStringBytes(s);
        } else {
            for (int i = 0; i < len; i++) {
                writeByte(s.charAt(i));
            }
        }
    }

    private void writeAsciiStringBytes(String s) throws IOException {
        writeByteArray(StringIntrospection.fastAsciiBytes(s));
    }

    /** {@inheritDoc} */
    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();

        for (int i = 0; i < len; i++) {
            writeChar(s.charAt(i));
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeUTF(String s) throws IOException {
        writeUtf(s, utfLength(s));
    }

    /**
     * Writes the given string in UTF format. This method is used in
     * situations where the UTF encoding length of the string is already
     * known; specifying it explicitly avoids a prescan of the string to
     * determine its UTF length.
     *
     * @param s String.
     * @param utfLen UTF length encoding.
     * @throws IOException In case of error.
     */
    private void writeUtf(String s, int utfLen) throws IOException {
        VarInts.writeUnsignedInt(utfLen, this);

        if (utfLen == s.length()) {
            writeAsciiStringBytes(s);
        } else {
            writeUtfBody(s);
        }
    }

    /**
     * Check for possible arithmetic overflow when trying to serialize a humongous array.
     *
     * @param bytes Number of bytes in a single array element.
     * @param arrLen Array length.
     * @param type Type of an array.
     * @throws IOException If oveflow presents and data corruption can occur.
     */
    private void checkArrayAllocationOverflow(int bytes, int arrLen, String type) throws IOException {
        long bytesToAlloc = (long) arrLen * bytes;

        if (!canBeAllocated(bytesToAlloc)) {
            throw new IOException("Failed to allocate required memory for " + type + " array "
                    + "(byte array size overflow detected) [length=" + arrLen + ']');
        }
    }

    /**
     * Returns the length in bytes of the UTF encoding of the given string.
     *
     * @param s String.
     * @return UTF encoding length.
     */
    private int utfLength(String s) {
        int len = s.length();
        int utfLen = 0;

        for (int off = 0; off < len; ) {
            int size = Math.min(len - off, CHAR_BUF_SIZE);

            s.getChars(off, off + size, cbuf, 0);

            for (int pos = 0; pos < size; pos++) {
                char c = cbuf[pos];

                if (c >= 0x0001 && c <= 0x007F) {
                    utfLen++;
                } else {
                    utfLen += c > 0x07FF ? 3 : 2;
                }
            }

            off += size;
        }

        return utfLen;
    }

    /**
     * Writes the "body" (i.e., the UTF representation minus the 2-byte or
     * 8-byte length header) of the UTF encoding for the given string.
     *
     * @param s String.
     * @throws IOException In case of error.
     */
    private void writeUtfBody(String s) throws IOException {
        int len = s.length();

        for (int off = 0; off < len; ) {
            int csize = Math.min(len - off, CHAR_BUF_SIZE);

            s.getChars(off, off + csize, cbuf, 0);

            for (int cpos = 0; cpos < csize; cpos++) {
                char c = cbuf[cpos];

                if (c <= 0x007F && c != 0) {
                    write(c);
                } else if (c > 0x07FF) {
                    write(0xE0 | ((c >> 12) & 0x0F));
                    write(0x80 | ((c >> 6) & 0x3F));
                    write(0x80 | ((c) & 0x3F));
                } else {
                    write(0xC0 | ((c >> 6) & 0x1F));
                    write(0x80 | ((c) & 0x3F));
                }
            }

            off += csize;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void flush() throws IOException {
        if (out != null) {
            out.flush();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return IgniteToStringBuilder.toString(IgniteUnsafeDataOutput.class, this);
    }
}
