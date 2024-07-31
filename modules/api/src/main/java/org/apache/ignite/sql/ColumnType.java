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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Predefined column types.
 */
public enum ColumnType {
    /** Null. */
    NULL(0, Void.class, false, false, false),

    /** Boolean. */
    BOOLEAN(1, Boolean.class, false, false, false),

    /** 8-bit signed integer. */
    INT8(2, Byte.class, false, false, false),

    /** 16-bit signed integer. */
    INT16(3, Short.class, false, false, false),

    /** 32-bit signed integer. */
    INT32(4, Integer.class, false, false, false),

    /** 64-bit signed integer. */
    INT64(5, Long.class, false, false, false),

    /** 32-bit single-precision floating-point number. */
    FLOAT(6, Float.class, false, false, false),

    /**
     * 64-bit double-precision floating-point number.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 31, implementation-defined precision
     */
    DOUBLE(7, Double.class, false, false, false),

    /** Arbitrary-precision signed decimal number. */
    DECIMAL(8, BigDecimal.class, true, true, false),

    /** Timezone-free date. */
    DATE(9, LocalDate.class, false, false, false),

    /** Timezone-free time with precision. */
    TIME(10, LocalTime.class, true, false, false),

    /** Timezone-free datetime. */
    DATETIME(11, LocalDateTime.class, true, false, false),

    /** Point on the time-line. Number of ticks since {@code 1970-01-01T00:00:00Z}. Tick unit depends on precision. */
    TIMESTAMP(12, Instant.class, true, false, false),

    /** 128-bit UUID. */
    UUID(13, UUID.class, false, false, false),

    // UNUSED id = 14

    /** String. */
    STRING(15, String.class, false, false, true),

    /** Binary data. */
    BYTE_ARRAY(16, byte[].class, false, false, true),

    /** Date interval. */
    PERIOD(17, Period.class, true, false, false),

    /** Time interval. */
    DURATION(18, Duration.class, true, false, false);

    private final Class<?> javaClass;
    private final boolean precisionAllowed;
    private final boolean scaleAllowed;
    private final boolean lengthAllowed;

    private final int id;

    private static ColumnType[] VALS;

    static {
        int maxId = Arrays.stream(values()).mapToInt(ColumnType::id).max().orElse(0);
        ColumnType[] vals = new ColumnType[maxId + 1];

        for (ColumnType columnType : values()) {
            ColumnType existing = vals[columnType.id];
            assert existing == null : "Found duplicate id " + columnType.id;
            vals[columnType.id] = columnType;
        }

        VALS = vals;
    }

    ColumnType(int id, Class<?> clazz, boolean precisionDefined, boolean scaleDefined, boolean lengthDefined) {
        assert !lengthDefined || (!precisionDefined && !scaleDefined);

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
    public static @Nullable ColumnType getById(int id) {
        return id >= 0 && id < VALS.length ? VALS[id] : null;
    }
}
