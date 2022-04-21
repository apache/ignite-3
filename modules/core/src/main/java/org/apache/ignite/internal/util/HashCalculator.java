/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.math.BigInteger;
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
     */
    public void append(Object v) {
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
            appendDecimal((BigDecimal) v);
        } else if (v.getClass() == UUID.class) {
            appendUuid((UUID) v);
        } else if (v.getClass() == LocalDate.class) {
            appendDate((LocalDate) v);
        } else if (v.getClass() == LocalTime.class) {
            appendTime((LocalTime) v);
        } else if (v.getClass() == LocalDateTime.class) {
            appendDateTime((LocalDateTime) v);
        } else if (v.getClass() == Instant.class) {
            appendTimestamp((Instant) v);
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
        hash = HashUtils.hash32(v, hash);
    }

    /**
     * Append short to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendShort(short v) {
        hash = HashUtils.hash32(v, hash);
    }

    /**
     * Append integer to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendInt(int v) {
        hash = HashUtils.hash32(v, hash);
    }

    /**
     * Append long to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendLong(long v) {
        hash = HashUtils.hash32(v, hash);
    }

    /**
     * Append float to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendFloat(float v) {
        appendInt(Float.floatToRawIntBits(v));
    }

    /**
     * Append double to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDouble(double v) {
        appendLong(Double.doubleToRawLongBits(v));
    }

    /**
     * Append BigDecimal to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDecimal(BigDecimal v) {
        appendDouble(v.doubleValue());
    }

    /**
     * Append BigInteger to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendNumber(BigInteger v) {
        appendDouble(v.doubleValue());
    }

    /**
     * Append UUID to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendUuid(UUID v) {
        appendLong(v.getMostSignificantBits());
        appendLong(v.getLeastSignificantBits());
    }

    /**
     * Append string value to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendString(String v) {
        appendBytes(v.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Append byte array to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendBytes(byte[] v) {
        hash = HashUtils.hash32(v, 0, v.length, hash);
    }

    /**
     * Append BitSet to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendBitmask(BitSet v) {
        appendBytes(v.toByteArray());
    }

    /**
     * Append LocalDate to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDate(LocalDate v) {
        appendLong(v.getYear());
        appendLong(v.getMonthValue());
        appendLong(v.getDayOfMonth());
    }

    /**
     * Append LocalTime to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendTime(LocalTime v) {
        appendLong(v.getHour());
        appendLong(v.getMinute());
        appendLong(v.getSecond());
        appendLong(v.getNano());
    }

    /**
     * Append LocalDateTime to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendDateTime(LocalDateTime v) {
        appendDate(v.toLocalDate());
        appendTime(v.toLocalTime());
    }

    /**
     * Append Instant to hash calculation.
     *
     * @param v Value to update hash.
     */
    public void appendTimestamp(Instant v) {
        appendLong(v.getEpochSecond());
        appendLong(v.getNano());
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
