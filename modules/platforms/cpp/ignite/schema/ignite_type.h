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

#include "types.h"

#include <ostream>

namespace ignite {

/**
 * @brief Supported types for stored objects.
 *
 * @note type of enum value is int, size is same as sizeof(int).
 */
enum class ignite_type {
    INT8 = 1, /**< 1-byte signed integer. */
    INT16 = 2, /**< 2-byte signed integer. */
    INT32 = 3, /**< 4-byte signed integer. */
    INT64 = 4, /**< 8-byte signed integer. */
    FLOAT = 5, /**< 4-byte floating-point number. */
    DOUBLE = 6, /**< 8-byte floating-point number. */
    DECIMAL = 7, /**< Variable-length fixed-point number. */
    UUID = 8, /**< UUID (16 bytes). */
    STRING = 9, /**< A UTF-8 encoded string. */
    BINARY = 10, /**< Variable-size byte array. */
    BITMASK = 11, /**< A fixed-length bitmask of n bits. */
    DATE = 12, /**< A timezone-free date encoded into 3 bytes as a year (1 sign bit + 14 bits), month
               (4 bits), day (5 bits). */
    TIME = 13, /**< A timezone-free time encoded into 5 bytes as padding (3 bits), hour
               (5 bits), minute (6 bits), second (6 bits), microsecond (20 bits). */
    DATETIME = 14, /**< A timezone-free datetime encoded into 8 bytes as (date, time). */
    TIMESTAMP = 15, /**< Number of microseconds since Jan 1, 1970 00:00:00.000000 (with no
                    timezone) encoded into 10 bytes. */
    NUMBER = 16, /**< Variable-length integer number (optionally bound by n bytes in size). */
    LAST /**< Can be used internally for input data range checks. */
};

/**
 * @brief Returns true if type has fixed size in bytes.
 *
 * @param t Type to investigate.
 * @return true if type has fixed size.
 * @return false if type has variable size.
 */
bool is_fixed_size_type(ignite_type t);

/**
 * @brief Returns size of type if it has fixed size.
 *
 * @param t Type to investigate.
 * @return Size in bytes.
 */
SizeT get_type_size(ignite_type t);

/**
 * @brief Writes a ignite_type value to an output stream.
 */
inline std::ostream &operator<<(std::ostream &os, const ignite_type t) {
    os << static_cast<std::underlying_type<ignite_type>::type>(t);
    return os;
}

} // namespace ignite
