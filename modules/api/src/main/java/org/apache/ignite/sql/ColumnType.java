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
    BOOLEAN,

    /** 8-bit signed integer. */
    INT8,

    /** 16-bit signed integer. */
    INT16,

    /** 32-bit signed integer. */
    INT32,

    /** 64-bit signed integer. */
    INT64,

    /** 32-bit single-precision floating-point number. */
    FLOAT,

    /** 64-bit double-precision floating-point number. */
    DOUBLE,

    /** Arbitrary-precision signed decimal number. */
    DECIMAL,

    /** Timezone-free date. */
    DATE,

    /** Timezone-free time with precision. */
    TIME,

    /** Timezone-free datetime. */
    DATETIME,

    /** Point on the time-line. Number of ticks since {@code 1970-01-01T00:00:00Z}. Tick unit depends on precision. */
    TIMESTAMP,

    /** 128-bit UUID. */
    UUID,

    /** Bit mask. */
    BITMASK,

    /** String. */
    STRING,

    /** Binary data. */
    BYTE_ARRAY,

    /** Date interval. */
    PERIOD,

    /** Time interval. */
    DURATION,

    /** Number. */
    NUMBER,

    /** Null. */
    NULL;

    /**
     * Column type to Java class.
     */
    public static Class<?> columnTypeToClass(ColumnType type) {
        assert type != null;

        switch (type) {
            case BOOLEAN:
                return Boolean.class;

            case INT8:
                return Byte.class;

            case INT16:
                return Short.class;

            case INT32:
                return Integer.class;

            case INT64:
                return Long.class;

            case FLOAT:
                return Float.class;

            case DOUBLE:
                return Double.class;

            case NUMBER:
                return BigInteger.class;

            case DECIMAL:
                return BigDecimal.class;

            case UUID:
                return UUID.class;

            case STRING:
                return String.class;

            case BYTE_ARRAY:
                return byte[].class;

            case BITMASK:
                return BitSet.class;

            case DATE:
                return LocalDate.class;

            case TIME:
                return LocalTime.class;

            case DATETIME:
                return LocalDateTime.class;

            case TIMESTAMP:
                return Instant.class;

            case PERIOD:
                return Period.class;

            case DURATION:
                return Duration.class;

            case NULL:
                return Void.class;

            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }
}
