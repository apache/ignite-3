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
    BOOLEAN(Boolean.class, false, false, false),

    /** 8-bit signed integer. */
    INT8(Byte.class, false, false, false),

    /** 16-bit signed integer. */
    INT16(Short.class, false, false, false),

    /** 32-bit signed integer. */
    INT32(Integer.class, false, false, false),

    /** 64-bit signed integer. */
    INT64(Long.class, false, false, false),

    /** 32-bit single-precision floating-point number. */
    FLOAT(Float.class, true, false, false),

    /**
     * 64-bit double-precision floating-point number.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 31, implementation-defined precision
     */
    DOUBLE(Double.class, false, false, false),

    /** Arbitrary-precision signed decimal number. */
    DECIMAL(BigDecimal.class, true, true, false),

    /** Timezone-free date. */
    DATE(LocalDate.class, false, false, false),

    /** Timezone-free time with precision. */
    TIME(LocalTime.class, true, false, false),

    /** Timezone-free datetime. */
    DATETIME(LocalDateTime.class, true, false, false),

    /** Point on the time-line. Number of ticks since {@code 1970-01-01T00:00:00Z}. Tick unit depends on precision. */
    TIMESTAMP(Instant.class, true, false, false),

    /** 128-bit UUID. */
    UUID(UUID.class, false, false, false),

    /** Bit mask. */
    BITMASK(BitSet.class, false, false, true),

    /** String. */
    STRING(String.class, false, false, true),

    /** Binary data. */
    BYTE_ARRAY(byte[].class, false, false, true),

    /** Date interval. */
    PERIOD(Period.class, true, false, false),

    /** Time interval. */
    DURATION(Duration.class, true, false, false),

    /** Number. */
    NUMBER(BigInteger.class, true, false, false),

    /** Null. */
    NULL(Void.class, false, false, false);

    private final Class<?> javaClass;
    private final boolean precisionAllowed;
    private final boolean scaleAllowed;
    private final boolean lengthAllowed;

    ColumnType(Class<?> clazz, boolean precisionDefined, boolean scaleDefined, boolean lengthDefined) {
        javaClass = clazz;
        this.precisionAllowed = precisionDefined;
        this.scaleAllowed = scaleDefined;
        this.lengthAllowed = lengthDefined;
    }

    /** Appropriate java match type. */
    public Class<?> javaClass() {
        return javaClass;
    }

    /** If {@code true} precision need to be specified, {@code false} otherwise. */
    public boolean precisionAllowed() {
        return precisionAllowed;
    }

    /** If {@code true} scale need to be specified, {@code false} otherwise. */
    public boolean scaleAllowed() {
        return scaleAllowed;
    }

    /** If {@code true} length need to be specified, {@code false} otherwise. */
    public boolean lengthAllowed() {
        return lengthAllowed;
    }
}
