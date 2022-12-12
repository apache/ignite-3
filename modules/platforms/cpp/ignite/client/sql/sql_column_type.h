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

#pragma once

namespace ignite {

/**
 * SQL column type.
 */
enum class column_type {
    /** Boolean. */
    BOOLEAN = 0,

    /** 8-bit signed integer. */
    INT8 = 1,

    /** 16-bit signed integer. */
    INT16 = 2,

    /** 32-bit signed integer. */
    INT32 = 3,

    /** 64-bit signed integer. */
    INT64 = 4,

    /** 32-bit single-precision floating-point number. */
    FLOAT = 5,

    /** 64-bit double-precision floating-point number. */
    DOUBLE = 6,

    /** A decimal floating-point number. */
    DECIMAL = 7,

    /** Timezone-free date. */
    DATE = 8,

    /** Timezone-free time with precision. */
    TIME = 9,

    /** Timezone-free datetime. */
    DATETIME = 10,

    /** Number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone). Tick unit depends on precision. */
    TIMESTAMP = 11,

    /** 128-bit UUID. */
    UUID = 12,

    /** Bit mask. */
    BITMASK = 13,

    /** String. */
    STRING = 14,

    /** Binary data. */
    BYTE_ARRAY = 15,

    /** Date interval. */
    PERIOD = 16,

    /** Time interval. */
    DURATION = 17,

    /** Number. */
    NUMBER = 18,

    /** Undefined. */
    UNDEFINED
};

} // namespace ignite
