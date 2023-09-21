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

package org.apache.ignite.sql;

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
 * Predefined column types.
 */
public enum ColumnType {
    /** Boolean. */
    BOOLEAN(Boolean.class, PrecisionScale.NO_NO, false),

    /** 8-bit signed integer. */
    INT8(Byte.class, PrecisionScale.NO_NO, false),

    /** 16-bit signed integer. */
    INT16(Short.class, PrecisionScale.NO_NO, false),

    /** 32-bit signed integer. */
    INT32(Integer.class, PrecisionScale.NO_NO, false),

    /** 64-bit signed integer. */
    INT64(Long.class, PrecisionScale.NO_NO, false),

    /** 32-bit single-precision floating-point number. */
    FLOAT(Float.class, PrecisionScale.YES_NO, false),

    /**
     * 64-bit double-precision floating-point number.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 31, implementation-defined precision
     */
    DOUBLE(Double.class, PrecisionScale.NO_NO, false),

    /** Arbitrary-precision signed decimal number. */
    DECIMAL(BigDecimal.class, PrecisionScale.YES_YES, false),

    /** Timezone-free date. */
    DATE(LocalDate.class, PrecisionScale.NO_NO, false),

    /** Timezone-free time with precision. */
    TIME(LocalTime.class, PrecisionScale.YES_NO, false),

    /** Timezone-free datetime. */
    DATETIME(LocalDateTime.class, PrecisionScale.YES_NO, false),

    /** Point on the time-line. Number of ticks since {@code 1970-01-01T00:00:00Z}. Tick unit depends on precision. */
    TIMESTAMP(Instant.class, PrecisionScale.YES_NO, false),

    /** 128-bit UUID. */
    UUID(UUID.class, PrecisionScale.NO_NO, false),

    /** Bit mask. */
    BITMASK(BitSet.class, PrecisionScale.NO_NO, true),

    /** String. */
    STRING(String.class, PrecisionScale.NO_NO, true),

    /** Binary data. */
    BYTE_ARRAY(byte[].class, PrecisionScale.NO_NO, true),

    /** Date interval. */
    PERIOD(Period.class, PrecisionScale.YES_NO, false),

    /** Time interval. */
    DURATION(Duration.class, PrecisionScale.YES_NO, false),

    /** Number. */
    NUMBER(BigInteger.class, PrecisionScale.YES_YES, false),

    /** Null. */
    NULL(Void.class, PrecisionScale.NO_NO, false);

    private final Class<?> javaClass;
    private final PrecisionScale precScale;
    private final boolean lengthSpecify;

    ColumnType(Class<?> clazz, PrecisionScale precScale, boolean len) {
        javaClass = clazz;
        this.precScale = precScale;
        lengthSpecify = len;
    }

    /** Appropriate java match type. */
    public Class<?> javaClass() {
        return javaClass;
    }

    /** Precision and scale definition. */
    public PrecisionScale precScale() {
        return precScale;
    }

    /** Return {@code true} if LENGTH can be specified. */
    public boolean specifiedLength() {
        return lengthSpecify;
    }

    /** Precision\scale status. */
    public enum PrecisionScale {
        /** Precision and scale not acceptable. */
        NO_NO,

        /** Only precision acceptable. */
        YES_NO,

        /** Precision and scale are both acceptable. */
        YES_YES;
    }
}
