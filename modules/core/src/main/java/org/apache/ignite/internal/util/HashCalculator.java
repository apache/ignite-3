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

import static org.apache.ignite.internal.util.HashUtils.combine;

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
        if (v == null) {
            appendNull();
            return;
        }

        if (v.getClass() == Boolean.class) {
            appendBoolean((boolean) v);
        } else if (v.getClass() == Byte.class) {
            appendByte((byte) v);
        } else if (v.getClass() == Short.class) {
            appendShort((short) v);
        } else if (v.getClass() == Integer.class) {
            appendInt((int) v);
        } else if (v.getClass() == Long.class) {
            appendLong((long) v);
        } else if (v.getClass() == Float.class) {
            appendFloat((float) v);
        } else if (v.getClass() == Double.class) {
            appendDouble((Double) v);
        } else if (v.getClass() == BigDecimal.class) {
            appendDecimal((BigDecimal) v, scale);
        } else if (v.getClass() == UUID.class) {
            appendUuid((UUID) v);
        } else if (v.getClass() == LocalDate.class) {
            appendDate((LocalDate) v);
        } else if (v.getClass() == LocalTime.class) {
            appendTime((LocalTime) v, precision);
        } else if (v.getClass() == LocalDateTime.class) {
            appendDateTime((LocalDateTime) v, precision);
        } else if (v.getClass() == Instant.class) {
            appendTimestamp((Instant) v, precision);
        } else if (v.getClass() == byte[].class) {
            appendBytes((byte[]) v);
        } else if (v.getClass() == String.class) {
            appendString((String) v);
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
        hash = combine(hash, hashBoolean(v));
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
        hash = combine(hash, hashByte(v));
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
        hash = combine(hash, hashShort(v));
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
        hash = combine(hash, hashInt(v));
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
        hash = combine(hash, hashLong(v));
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
        hash = combine(hash, hashFloat(v));
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
        hash = combine(hash, hashDouble(v));
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
        hash = combine(hash, hashDecimal(v, columnScale));
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
        hash = combine(hash, hashUuid(v));
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
        hash = combine(hash, hashString(v));
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
        hash = combine(hash, hashBytes(v));
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
        hash = combine(hash, hashDate(v));
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
        hash = combine(hash, hashTime(v, precision));
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
        hash = combine(hash, hashDateTime(v, precision));
    }

    /**
     * Get value hash.
     *
     * @param v Value to hash.
     */
    public static int hashDateTime(LocalDateTime v, int precision) {
        return combine(hashDate(v.toLocalDate()), hashTime(v.toLocalTime(), precision));
    }

    /**
     * Append Instant to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendTimestamp(Instant v, int precision) {
        hash = combine(hash, hashTimestamp(v, precision));
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
     * Get сombined hash code.
     *
     * @param hashes Individual hash codes.
     */
    public static int combinedHash(int[] hashes) {
        int hash = 0;

        for (int h : hashes) {
            hash = combine(hash, h);
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
