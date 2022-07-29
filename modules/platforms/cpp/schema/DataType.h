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

#pragma once

#include <ostream>
#include <stdexcept>

namespace ignite {

/**
 * @brief Supported types for stored objects.
 *
 * @note type of enum value is int, size is same as sizeof(int).
 */
enum class DATA_TYPE {
    INT8, /**< 1-byte signed integer. */
    INT16, /**< 2-byte signed integer. */
    INT32, /**< 4-byte signed integer. */
    INT64, /**< 8-byte signed integer. */
    FLOAT, /**< 4-byte floating-point number. */
    DOUBLE, /**< 8-byte floating-point number. */
    NUMBER, /**< Variable-length integer number (optionally bound by n bytes in size). */
    DECIMAL, /**< Variable-length fixed-point number. */
    STRING, /**< A UTF-8 encoded string. */
    BINARY, /**< Variable-size byte array. */
    BITMASK, /**< A fixed-length bitmask of n bits. */
    UUID, /**< UUID (16 bytes). */
    DATE, /**< A timezone-free date encoded into 3 bytes as a year (1 sign bit + 14 bits), month
               (4 bits), day (5 bits). */
    TIME, /**< A timezone-free time encoded into 5 bytes as padding (3 bits), hour
               (5 bits), minute (6 bits), second (6 bits), microsecond (20 bits). */
    DATETIME, /**< A timezone-free datetime encoded into 8 bytes as (date, time). */
    TIMESTAMP, /**< Number of microseconds since Jan 1, 1970 00:00:00.000000 (with no
                    timezone) encoded into 10 bytes. */
    LAST /**< Can be used internally for input data range checks. */
};

/**
 * @brief returns true if type has fixed size in bytes
 *
 * @param t Type to investigate.
 * @return true if type has fixed size.
 * @return false if type has variable size.
 */
constexpr bool isFixedSizeType(DATA_TYPE t) {
    switch (t) {
        case DATA_TYPE::INT8:
        case DATA_TYPE::INT16:
        case DATA_TYPE::INT32:
        case DATA_TYPE::INT64:
        case DATA_TYPE::FLOAT:
        case DATA_TYPE::DOUBLE:
        case DATA_TYPE::UUID:
        case DATA_TYPE::DATE:
        case DATA_TYPE::TIME:
        case DATA_TYPE::DATETIME:
        case DATA_TYPE::TIMESTAMP:
            return true;
        default:
            return false;
    }
}

/**
 * @brief gets size of type if it has fixed size
 *
 * @param t Type to investigate.
 * @return size_t Size in bytes.
 */
constexpr size_t getTypeSize(DATA_TYPE t) {
    switch (t) {
        case DATA_TYPE::INT8:
            return 1;
        case DATA_TYPE::INT16:
            return 2;
        case DATA_TYPE::INT32:
            return 4;
        case DATA_TYPE::INT64:
            return 8;
        case DATA_TYPE::FLOAT:
            return 4;
        case DATA_TYPE::DOUBLE:
            return 8;
        case DATA_TYPE::UUID:
            return 16;
        case DATA_TYPE::DATE:
            return 3;
        case DATA_TYPE::TIME:
            return 5;
        case DATA_TYPE::DATETIME:
            return 8;
        case DATA_TYPE::TIMESTAMP:
            return 10;
        case DATA_TYPE::BITMASK:
        case DATA_TYPE::NUMBER:
        case DATA_TYPE::DECIMAL:
        case DATA_TYPE::STRING:
        case DATA_TYPE::BINARY:
            /* Only fixed size types are supported for now. */
            throw std::logic_error("Can't get size of variable-size type id " + std::to_string(static_cast<int>(t)));
        default:
            throw std::logic_error("Unsupported type id " + std::to_string(static_cast<int>(t)) + " in schema");
    }
}

/**
 * @brief writes a DATA_TYPE value to an output stream
 */
inline std::ostream &operator<<(std::ostream &os, const DATA_TYPE &t) {
    os << static_cast<std::underlying_type<DATA_TYPE>::type>(t);
    return os;
}

} // namespace ignite
