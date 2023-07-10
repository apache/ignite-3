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
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;

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
    public void append(Object v, int scale, int precision) {
        if (v == null) {
            appendNull();
            return;
        }

        if (v.getClass() == Byte.class) {
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
        } else if (v.getClass() == BigInteger.class) {
            appendNumber((BigInteger) v);
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
        } else if (v.getClass() == BitSet.class) {
            appendBitmask((BitSet) v);
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
     * Append byte to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendByte(byte v) {
        hash = combine(hash, HashUtils.hash32(v, 0));
    }

    /**
     * Append short to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendShort(short v) {
        hash = combine(hash, HashUtils.hash32(v, 0));
    }

    /**
     * Append integer to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendInt(int v) {
        hash = combine(hash, HashUtils.hash32(v, 0));
    }

    /**
     * Append long to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendLong(long v) {
        hash = combine(hash, HashUtils.hash32(v, 0));
    }

    /**
     * Append float to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendFloat(float v) {
        hash = combine(hash, HashUtils.hash32(Float.floatToRawIntBits(v), 0));
    }

    /**
     * Append double to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDouble(double v) {
        hash = combine(hash, HashUtils.hash32(Double.doubleToRawLongBits(v), 0));
    }

    /**
     * Append BigDecimal to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDecimal(BigDecimal v, int columnScale) {
        byte[] v1 = v.setScale(columnScale, RoundingMode.HALF_UP).unscaledValue().toByteArray();
        hash = combine(hash, HashUtils.hash32(v1, 0, v1.length, 0));
    }

    /**
     * Append BigInteger to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendNumber(BigInteger v) {
        byte[] v1 = v.toByteArray();
        hash = combine(hash, HashUtils.hash32(v1, 0, v1.length, 0));
    }

    /**
     * Append UUID to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendUuid(UUID v) {
        int hash1 = HashUtils.hash32(v.getMostSignificantBits(), 0);
        int hash2 = HashUtils.hash32(v.getLeastSignificantBits(), 0);

        int vHash = combine(hash1, hash2);
        hash = combine(hash, vHash);
    }

    /**
     * Append string value to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendString(String v) {
        byte[] v1 = v.getBytes(StandardCharsets.UTF_8);
        hash = combine(hash, HashUtils.hash32(v1, 0, v1.length, 0));
    }

    /**
     * Append byte array to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendBytes(byte[] v) {
        hash = combine(hash, HashUtils.hash32(v, 0, v.length, 0));
    }

    /**
     * Append BitSet to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendBitmask(BitSet v) {
        byte[] v1 = v.toByteArray();
        hash = combine(hash, HashUtils.hash32(v1, 0, v1.length, 0));
    }

    /**
     * Append LocalDate to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDate(LocalDate v) {
        hash = combine(hash, hash(v));
    }

    private static int hash(LocalDate v) {
        int yearHash = HashUtils.hash32(v.getYear(), 0);
        int monthHash = HashUtils.hash32(v.getMonthValue(), 0);
        int dayHash = HashUtils.hash32(v.getDayOfMonth(), 0);

        return combine(yearHash, monthHash, dayHash);
    }

    /**
     * Append LocalTime to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendTime(LocalTime v, int precision) {
        hash = combine(hash, hash(v, precision));
    }

    private static int hash(LocalTime v, int precision) {
        int hourHash = HashUtils.hash32(v.getHour(), 0);
        int minuteHash = HashUtils.hash32(v.getMinute(), 0);
        int secondHash = HashUtils.hash32(v.getSecond(), 0);
        int nanoHash = HashUtils.hash32(TemporalTypeUtils.normalizeNanos(v.getNano(), precision), 0);

        return combine(hourHash, minuteHash, secondHash, nanoHash);
    }

    /**
     * Append LocalDateTime to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendDateTime(LocalDateTime v, int precision) {
        int vHash = combine(hash(v.toLocalDate()), hash(v.toLocalTime(), precision));
        hash = combine(hash, vHash);
    }

    /**
     * Append Instant to hash calculation.
     *
     * @param v Value to update hash.
     * @param precision Precision.
     */
    public void appendTimestamp(Instant v, int precision) {
        int hash1 = HashUtils.hash32(v.getEpochSecond(), 0);
        int hash2 = HashUtils.hash32(TemporalTypeUtils.normalizeNanos(v.getNano(), precision), 0);

        int vHash = combine(hash1, hash2);
        hash = combine(hash, vHash);
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
