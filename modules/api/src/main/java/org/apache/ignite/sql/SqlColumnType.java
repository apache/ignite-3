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

package org.apache.ignite.sql;

/**
 * Predefined column types.
 */
public enum SqlColumnType {
    /** Boolaen. */
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

    /** A decimal floating-point number. */
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
    NUMBER
}
