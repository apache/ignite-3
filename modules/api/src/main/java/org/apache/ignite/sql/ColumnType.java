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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Predefined column types.
 */
public enum ColumnType {
    /** Null. */
    NULL(Void.class, false, false, false, 0),

    /** Boolean. */
    BOOLEAN(Boolean.class, false, false, false, 1),

    /** 8-bit signed integer. */
    INT8(Byte.class, false, false, false, 2),

    /** 16-bit signed integer. */
    INT16(Short.class, false, false, false, 3),

    /** 32-bit signed integer. */
    INT32(Integer.class, false, false, false, 4),

    /** 64-bit signed integer. */
    INT64(Long.class, false, false, false, 5),

    /** 32-bit single-precision floating-point number. */
    FLOAT(Float.class, false, false, false, 6),

    /**
     * 64-bit double-precision floating-point number.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 31, implementation-defined precision
     */
    DOUBLE(Double.class, false, false, false, 7),

    /** Arbitrary-precision signed decimal number. */
    DECIMAL(BigDecimal.class, true, true, false, 8),

    /** Timezone-free date. */
    DATE(LocalDate.class, false, false, false, 9),

    /** Timezone-free time with precision. */
    TIME(LocalTime.class, true, false, false, 10),

    /** Timezone-free datetime. */
    DATETIME(LocalDateTime.class, true, false, false, 11),

    /** Point on the time-line. Number of ticks since {@code 1970-01-01T00:00:00Z}. Tick unit depends on precision. */
    TIMESTAMP(Instant.class, true, false, false, 12),

    /** 128-bit UUID. */
    UUID(UUID.class, false, false, false, 13),

    /** Bit mask. */
    BITMASK(BitSet.class, false, false, true, 14),

    /** String. */
    STRING(String.class, false, false, true, 15),

    /** Binary data. */
    BYTE_ARRAY(byte[].class, false, false, true, 16),

    /** Date interval. */
    PERIOD(Period.class, true, false, false, 17),

    /** Time interval. */
    DURATION(Duration.class, true, false, false, 18),

    /** Number. */
    NUMBER(BigInteger.class, true, false, false, 19);

    private final Class<?> javaClass;
    private final boolean precisionAllowed;
    private final boolean scaleAllowed;
    private final boolean lengthAllowed;

    private final int id;

    private static final ColumnType[] VALS = new ColumnType[values().length];

    static {
        for (ColumnType columnType : values()) {
            assert VALS[columnType.id] == null : "Found duplicate id " + columnType.id;
            VALS[columnType.id()] = columnType;
        }
    }

    ColumnType(Class<?> clazz, boolean precisionDefined, boolean scaleDefined, boolean lengthDefined, int id) {
        javaClass = clazz;
        this.precisionAllowed = precisionDefined;
        this.scaleAllowed = scaleDefined;
        this.lengthAllowed = lengthDefined;
        this.id = id;
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

    /** Returns id of type. */
    public int id() {
        return id;
    }

    /** Returns corresponding {@code ColumnType} by given id, {@code null} for unknown id. */
    @Nullable
    public static ColumnType getById(int id) {
        return id >= 0 && id < VALS.length ? VALS[id] : null;
    }
}
