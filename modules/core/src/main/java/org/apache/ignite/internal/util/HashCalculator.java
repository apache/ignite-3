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

package org.apache.ignite.internal.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Implement hash calculator.
 */
public class HashCalculator {
    private int hash = 0;

    /**
     * Append value to hash calculation.
     *
     * @param v Value to update hash.
     * @param scale Decimal scale.
     * @param precision Temporal precision.
     */
    public void append(@Nullable Object v, int scale, int precision) {
        combine(hashValue(v, scale, precision));
    }

    /** Generates 32-bit hash from the given value with regard to provided scale and precision. */
    public static int hashValue(@Nullable Object v, int scale, int precision) {
        if (v == null) {
            return hashByte((byte) 0);
        }

        if (v.getClass() == Boolean.class) {
            return hashBoolean((boolean) v);
        } else if (v.getClass() == Byte.class) {
            return hashByte((byte) v);
        } else if (v.getClass() == Short.class) {
            return hashShort((short) v);
        } else if (v.getClass() == Integer.class) {
            return hashInt((int) v);
        } else if (v.getClass() == Long.class) {
            return hashLong((long) v);
        } else if (v.getClass() == Float.class) {
            return hashFloat((float) v);
        } else if (v.getClass() == Double.class) {
            return hashDouble((Double) v);
        } else if (v.getClass() == BigDecimal.class) {
            return hashDecimal((BigDecimal) v, scale);
        } else if (v.getClass() == UUID.class) {
            return hashUuid((UUID) v);
        } else if (v.getClass() == LocalDate.class) {
            return hashDate((LocalDate) v);
        } else if (v.getClass() == LocalTime.class) {
            return hashTime((LocalTime) v, precision);
        } else if (v.getClass() == LocalDateTime.class) {
            return hashDateTime((LocalDateTime) v, precision);
        } else if (v.getClass() == Instant.class) {
            return hashTimestamp((Instant) v, precision);
        } else if (v.getClass() == byte[].class) {
            return hashBytes((byte[]) v);
        } else if (v.getClass() == String.class) {
            return hashString((String) v);
        } else {
            throw new IgniteInternalException("Unsupported value type: [cls=" + v.getClass() + ']');
        }
    }

    /**
     * Append null value to hash calculation.
     */
    public void appendNull() {
        appendByte((byte) 0);
    }

    /**
     * Append boolean to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendBoolean(boolean v) {
        combine(hashBoolean(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashBoolean(boolean v) {
        return HashUtils.hash32(ByteUtils.booleanToByte(v));
    }

    /**
     * Append byte to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendByte(byte v) {
        combine(hashByte(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashByte(byte v) {
        return HashUtils.hash32(v);
    }

    /**
     * Append short to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendShort(short v) {
        combine(hashShort(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashShort(short v) {
        return HashUtils.hash32(v);
    }

    /**
     * Append integer to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendInt(int v) {
        combine(hashInt(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashInt(int v) {
        return HashUtils.hash32(v);
    }

    /**
     * Append long to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendLong(long v) {
        combine(hashLong(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashLong(long v) {
        return HashUtils.hash32(v);
    }

    /**
     * Append float to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendFloat(float v) {
        combine(hashFloat(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashFloat(float v) {
        return HashUtils.hash32(Float.floatToRawIntBits(v));
    }

    /**
     * Append double to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDouble(double v) {
        combine(hashDouble(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashDouble(double v) {
        return HashUtils.hash32(Double.doubleToRawLongBits(v));
    }

    /**
     * Append BigDecimal to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDecimal(BigDecimal v, int columnScale) {
        combine(hashDecimal(v, columnScale));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashDecimal(BigDecimal v, int columnScale) {
        return hashBytes(v.setScale(columnScale, RoundingMode.HALF_UP).unscaledValue().toByteArray());
    }

    /**
     * Append UUID to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendUuid(UUID v) {
        combine(hashUuid(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashUuid(UUID v) {
        return HashUtils.hash32(v.getLeastSignificantBits(), HashUtils.hash32(v.getMostSignificantBits()));
    }

    /**
     * Append string value to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendString(String v) {
        combine(hashString(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashString(String v) {
        return hashBytes(v.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Append byte array to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendBytes(byte[] v) {
        combine(hashBytes(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashBytes(byte[] v) {
        return HashUtils.hash32(v);
    }

    /**
     * Append LocalDate to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDate(LocalDate v) {
        combine(hashDate(v));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashDate(LocalDate v) {
        return HashUtils.hash32(v.getDayOfMonth(),
                HashUtils.hash32(v.getMonthValue(),
                        HashUtils.hash32(v.getYear())));
    }

    /**
     * Append LocalTime to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendTime(LocalTime v, int precision) {
        combine(hashTime(v, precision));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashTime(LocalTime v, int precision) {
        int hourHash = HashUtils.hash32(v.getHour());
        int minuteHash = HashUtils.hash32(v.getMinute(), hourHash);
        int secondHash = HashUtils.hash32(v.getSecond(), minuteHash);
        return HashUtils.hash32(TemporalTypeUtils.normalizeNanos(v.getNano(), precision), secondHash);
    }

    /**
     * Append LocalDateTime to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendDateTime(LocalDateTime v, int precision) {
        combine(hashDateTime(v, precision));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashDateTime(LocalDateTime v, int precision) {
        return HashUtils.combine(hashDate(v.toLocalDate()), hashTime(v.toLocalTime(), precision));
    }

    /**
     * Append Instant to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendTimestamp(Instant v, int precision) {
        combine(hashTimestamp(v, precision));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashTimestamp(Instant v, int precision) {
        return HashUtils.hash32(TemporalTypeUtils.normalizeNanos(v.getNano(), precision), hashLong(v.getEpochSecond()));
    }

    /**
     * Combines current state with hash calculated externally.
     *
     * <p>If external hash was calculated with {@link #hashValue(Object, int, int)}, then result of this operation will be the same as if
     * value was appended to this calculator. That is, following code will provide equal results:
     *
     * <pre>
     *         HashCalculator calc1 = new HashCalculator();
     *         HashCalculator calc2 = new HashCalculator();
     *
     *         // Update the sate of calculators somehow.
     *         ...
     *
     *         // Given the both calculators were prepared with
     *         // the same sequence of values, following two lines
     *         // will provide equal results.
     *
     *         calc1.append(value, scale, precision);
     *         calc2.combine(hashValue(value, scale, precision));
     *
     *         assertEquals(calc1.hash(), calc2.hash());
     * </pre>
     *
     * @param anotherHash Another hash to combine with current state.
     */
    public void combine(int anotherHash) {
        hash = HashUtils.combine(hash, anotherHash);
    }

    /**
     * Get сombined hash code.
     *
     * @param hashes Individual hash codes.
     */
    public static int combinedHash(int[] hashes) {
        int hash = 0;

        for (int h : hashes) {
            hash = HashUtils.combine(hash, h);
        }

        return hash;
    }

    /**
     * Hash code calculated by previous appended values.
     *
     * @return Hash code calculated by appended values.
     */
    public int hash() {
        return hash;
    }

    /**
     * Reset hash code calculation.
     */
    public void reset() {
        hash = 0;
    }
}
