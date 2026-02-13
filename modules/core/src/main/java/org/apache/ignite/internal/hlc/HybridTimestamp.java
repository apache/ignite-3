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

package org.apache.ignite.internal.hlc;

import static org.apache.ignite.internal.lang.JavaLoggerFormatter.DATE_FORMATTER;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.VarIntUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * A hybrid timestamp that combines physical clock and logical clock.
 *
 * <p>Serialization methods ({@link #toBytes()}, {@link #writeTo(IgniteDataOutput)}, {@link #write(HybridTimestamp, IgniteDataOutput)})
 * write a timestamp in the following format:
 *
 * <pre>{@code
 *   <TS> ::= <PHYSICAL_PART> <LOGICAL_PART>
 *
 *   <PHYSICAL_PART> ::= <PHYSICAL_HIGH> (4 bytes int) <PHYSICAL_LOW> (2 bytes short) // both are in little-endian
 *
 *   <LOGICAL_PART> ::= (varint)
 * }</pre>
 *
 * <p>A null timestamp is serialized as both parts (physical and logical) equal to 0.
 */
public final class HybridTimestamp implements Comparable<HybridTimestamp>, Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = -4285668148196042529L;

    /** Time value to store for {@code null} hybrid timestamp values. */
    public static final long NULL_HYBRID_TIMESTAMP = 0L;

    /** Number of bits in "logical time" part. */
    public static final int LOGICAL_TIME_BITS_SIZE = 2 * Byte.SIZE;

    /** Mask to extract logical time. */
    private static final long LOGICAL_TIME_MASK = (1L << LOGICAL_TIME_BITS_SIZE) - 1;

    /** Number of bits in "physical time" part. */
    static final int PHYSICAL_TIME_BITS_SIZE = 6 * Byte.SIZE;

    /** Timestamp size in bytes. */
    public static final int HYBRID_TIMESTAMP_SIZE = Long.BYTES;

    /** A constant holding the maximum value a {@code HybridTimestamp} can have. */
    public static final HybridTimestamp MAX_VALUE = new HybridTimestamp(Long.MAX_VALUE);

    /** The constant holds the minimum value which {@code HybridTimestamp} might formally have. */
    public static final HybridTimestamp MIN_VALUE = new HybridTimestamp(0L, 1);

    /** Long time value, that consists of physical time in higher 6 bytes and logical time in lower 2 bytes. */
    private final long time;

    /**
     * The constructor.
     *
     * @param physical The physical time.
     * @param logical The logical time.
     */
    public HybridTimestamp(long physical, int logical) {
        if (physical < 0 || physical >= (1L << PHYSICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("Physical time is out of bounds: " + physical);
        }

        if (logical < 0 || logical >= (1L << LOGICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("Logical time is out of bounds: " + logical);
        }

        time = (physical << LOGICAL_TIME_BITS_SIZE) | logical;

        // Negative time breaks comparison, we don't allow overflow of the physical time.
        // "0" is a reserved value for "NULL_HYBRID_TIMESTAMP".
        if (time <= 0) {
            throw new IllegalArgumentException("Time is out of bounds: " + time);
        }
    }

    /**
     * The constructor.
     *
     * @param time Long time value.
     */
    private HybridTimestamp(long time) {
        this.time = time;

        // Negative time breaks comparison, we don't allow overflow of the physical time.
        // "0" is a reserved value for "NULL_HYBRID_TIMESTAMP".
        if (time <= 0) {
            throw new IllegalArgumentException("Time is out of bounds: " + time);
        }
    }

    /**
     * Converts primitive {@code long} representation into a hybrid timestamp instance.
     * {@link #NULL_HYBRID_TIMESTAMP} is interpreted as {@code null}.
     *
     * @throws IllegalArgumentException If timestamp is negative.
     */
    public static @Nullable HybridTimestamp nullableHybridTimestamp(long time) {
        return time == NULL_HYBRID_TIMESTAMP ? null : new HybridTimestamp(time);
    }

    /**
     * Converts primitive {@code long} representation into a hybrid timestamp instance.
     *
     * @throws IllegalArgumentException If timestamp is not positive.
     */
    public static HybridTimestamp hybridTimestamp(long time) {
        return new HybridTimestamp(time);
    }

    /**
     * Converts hybrid timestamp instance to a primitive {@code long} representation.
     * {@code null} is represented as {@link #NULL_HYBRID_TIMESTAMP}.
     */
    public static long hybridTimestampToLong(@Nullable HybridTimestamp timestamp) {
        return timestamp == null ? NULL_HYBRID_TIMESTAMP : timestamp.time;
    }

    /**
     * Restores a timestamp converted to bytes using {@link #toBytes()}.
     *
     * <p>See the class javadoc for details on the serialization format.
     *
     * @param bytes Byte array representing a timestamp.
     * @see #toBytes()
     */
    public static HybridTimestamp fromBytes(byte[] bytes) {
        // Reversing bytes as ByteUtils works in BE, but we store in LE (as IgniteDataOutput uses LE and we want to be consistent between
        // serialization methods).
        long physicalHigh = Integer.reverseBytes(ByteUtils.bytesToInt(bytes, 0));
        int physicalLow = Short.reverseBytes(ByteUtils.bytesToShort(bytes, Integer.BYTES)) & 0xFFFF;

        long physical = (physicalHigh << Short.SIZE) | physicalLow;
        //noinspection NumericCastThatLosesPrecision
        int logical = (int) VarIntUtils.readVarInt(bytes, Integer.BYTES + Short.BYTES);

        assert physical != 0 || logical != 0;

        return new HybridTimestamp(physical, logical);
    }

    /**
     * Finds maximum hybrid timestamp.
     *
     * @param times Times for comparing. Must not be {@code null} or empty.
     * @return The highest hybrid timestamp.
     */
    public static HybridTimestamp max(HybridTimestamp... times) {
        assert times != null;
        assert times.length > 0;

        HybridTimestamp maxTime = times[0];

        for (int i = 1; i < times.length; i++) {
            if (maxTime.compareTo(times[i]) < 0) {
                maxTime = times[i];
            }
        }

        return maxTime;
    }

    /**
     * Returns a physical component.
     *
     * @return The physical component.
     */
    public long getPhysical() {
        return time >>> LOGICAL_TIME_BITS_SIZE;
    }

    /**
     * Returns a logical component.
     *
     * @return The logical component.
     */
    public int getLogical() {
        return (int) (time & LOGICAL_TIME_MASK);
    }

    /**
     * Returns a compressed representation as a primitive {@code long} value.
     */
    public long longValue() {
        return time;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return time == ((HybridTimestamp) o).time;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(time);
    }

    @Override
    public int compareTo(HybridTimestamp other) {
        return Long.compare(this.time, other.time);
    }

    @Override
    public String toString() {
        String formattedPhysicalTime = DATE_FORMATTER.format(Instant.ofEpochMilli(getPhysical()).atOffset(ZoneOffset.UTC));

        return "HybridTimestamp [physical=" + formattedPhysicalTime + ", logical=" + getLogical() + ", composite=" + time + "]";
    }

    /**
     * Returns a new hybrid timestamp with incremented physical component.
     */
    public HybridTimestamp addPhysicalTime(long millis) {
        if (millis >= (1L << PHYSICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("Physical time is out of bounds: " + millis);
        }

        return new HybridTimestamp(time + (millis << LOGICAL_TIME_BITS_SIZE));
    }

    /**
     * Returns a new {@link HybridTimestamp} that is greater than current by 1 logical tick.
     *
     * @return New {@link HybridTimestamp} that is greater than current by 1 logical tick.
     */
    public HybridTimestamp tick() {
        return hybridTimestamp(time + 1);
    }

    /**
     * Returns a new hybrid timestamp with decremented physical component.
     */
    public HybridTimestamp subtractPhysicalTime(long millis) {
        if (millis >= (1L << PHYSICAL_TIME_BITS_SIZE)) {
            throw new IllegalArgumentException("Physical time is out of bounds: " + millis);
        }

        return new HybridTimestamp(time - (millis << LOGICAL_TIME_BITS_SIZE));
    }

    /**
     * Returns a result of rounding this timestamp up 'to its physical part': that is, if the logical part is zero, the timestamp is
     * returned as is; if it's non-zero, a new timestamp is returned that has physical part equal to the physical part of this
     * timestamp plus one, and the logical part is zero.
     */
    public HybridTimestamp roundUpToPhysicalTick() {
        if (getLogical() == 0) {
            return this;
        } else {
            return new HybridTimestamp(getPhysical() + 1, 0);
        }
    }

    /**
     * Returns a byte array representing this timestamp.
     *
     * <p>See the class javadoc for details on the serialization format.
     *
     * @see #fromBytes(byte[])
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public byte[] toBytes() {
        long physical = getPhysical();
        int logical = getLogical();

        byte[] bytes = new byte[Integer.BYTES + Short.BYTES + VarIntUtils.varIntLength(logical)];

        // Reversing bytes as ByteUtils works in BE, but we store in LE (as IgniteDataOutput uses LE and we want to be consistent between
        // serialization methods).
        ByteUtils.putIntToBytes(Integer.reverseBytes((int) (physical >> Short.SIZE)), bytes, 0);
        ByteUtils.putShortToBytes(Short.reverseBytes((short) (physical & 0xFFFF)), bytes, Integer.BYTES);

        VarIntUtils.putVarIntToBytes(logical, bytes, Integer.BYTES + Short.BYTES);

        return bytes;
    }

    /**
     * Writes this timestamp to the output.
     *
     * <p>See the class javadoc for details on the serialization format.
     *
     * @param out Output to write to.
     * @throws IOException If something goes wrong.
     * @see #readFrom(IgniteDataInput)
     */
    public void writeTo(IgniteDataOutput out) throws IOException {
        long physical = getPhysical();

        //noinspection NumericCastThatLosesPrecision
        out.writeInt((int) (physical >> Short.SIZE));
        out.writeShort((int) (physical & 0xFFFF));

        out.writeVarInt(getLogical());
    }

    /**
     * Writes a nullable timestamp to an output.
     *
     * <p>See the class javadoc for details on the serialization format.
     *
     * @param timestamp Timestamp to write
     * @param out Output to write to.
     * @throws IOException If something goes wrong.
     * @see #readNullableFrom(IgniteDataInput)
     */
    public static void write(@Nullable HybridTimestamp timestamp, IgniteDataOutput out) throws IOException {
        if (timestamp == null) {
            out.writeInt(0);
            out.writeShort(0);
            out.writeVarInt(0);
        } else {
            timestamp.writeTo(out);
        }
    }

    /**
     * Reads a timestamp written with {@link #writeTo(IgniteDataOutput)}.
     *
     * <p>See the class javadoc for details on the serialization format.
     *
     * @param in Input from which to read.
     * @throws IOException If something goes wrong.
     * @see #write(HybridTimestamp, IgniteDataOutput)
     */
    public static @Nullable HybridTimestamp readNullableFrom(IgniteDataInput in) throws IOException {
        long physicalHigh = in.readInt();
        int physicalLow = in.readShort() & 0xFFFF;

        long physical = (physicalHigh << Short.SIZE) | physicalLow;
        int logical = in.readVarIntAsInt();

        if (physical == 0 && logical == 0) {
            return null;
        }

        return new HybridTimestamp(physical, logical);
    }

    /**
     * Reads a timestamp written with {@link #writeTo(IgniteDataOutput)}.
     *
     * <p>See the class javadoc for details on the serialization format.
     *
     * @param in Input from which to read.
     * @throws IOException If something goes wrong.
     * @see #writeTo(IgniteDataOutput)
     */
    public static HybridTimestamp readFrom(IgniteDataInput in) throws IOException {
        HybridTimestamp ts = readNullableFrom(in);

        if (ts == null) {
            throw new IOException("A non-null timestamp is expected");
        }

        return ts;
    }
}
