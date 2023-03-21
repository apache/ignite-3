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

#include <ignite/common/ignite_error.h>
#include <ignite/common/ignite_type.h>

namespace ignite::detail {

/**
 * Column data type encoding for the client protocol.
 */
struct client_data_type {
    /** Byte. */
    static constexpr std::int32_t INT8 = 1;

    /** Short. */
    static constexpr std::int32_t INT16 = 2;

    /** Int. */
    static constexpr std::int32_t INT32 = 3;

    /** Long. */
    static constexpr std::int32_t INT64 = 4;

    /** Float. */
    static constexpr std::int32_t FLOAT = 5;

    /** Double. */
    static constexpr std::int32_t DOUBLE = 6;

    /** Decimal. */
    static constexpr std::int32_t DECIMAL = 7;

    /** UUID. */
    static constexpr std::int32_t UUID = 8;

    /** String. */
    static constexpr std::int32_t STRING = 9;

    /** Byte array. */
    static constexpr std::int32_t BYTES = 10;

    /** BitMask. */
    static constexpr std::int32_t BITMASK = 11;

    /** Date. */
    static constexpr std::int32_t DATE = 12;

    /** Time. */
    static constexpr std::int32_t TIME = 13;

    /** DateTime. */
    static constexpr std::int32_t DATETIME = 14;

    /** Timestamp. */
    static constexpr std::int32_t TIMESTAMP = 15;

    /** Number. */
    static constexpr std::int32_t NUMBER = 16;

    /** Boolean. */
    static constexpr std::int32_t BOOLEAN = 17;

    /** Duration. */
    static constexpr std::int32_t DURATION = 18;

    /** Period. */
    static constexpr std::int32_t PERIOD = 19;

    /**
     * Get the ignite type matching a client data type value.
     *
     * @param t Client data type.
     * @return Matching ignite type.
     */
    static ignite_type to_ignite_type(std::int32_t t) {
        switch (t) {
            case INT8:
                return ignite_type::INT8;
            case INT16:
                return ignite_type::INT16;
            case INT32:
                return ignite_type::INT32;
            case INT64:
                return ignite_type::INT64;
            case FLOAT:
                return ignite_type::FLOAT;
            case DOUBLE:
                return ignite_type::DOUBLE;
            case DECIMAL:
                return ignite_type::DECIMAL;
            case UUID:
                return ignite_type::UUID;
            case STRING:
                return ignite_type::STRING;
            case BYTES:
                return ignite_type::BYTE_ARRAY;
            case BITMASK:
                return ignite_type::BITMASK;
            case DATE:
                return ignite_type::DATE;
            case TIME:
                return ignite_type::TIME;
            case DATETIME:
                return ignite_type::DATETIME;
            case TIMESTAMP:
                return ignite_type::TIMESTAMP;
            case NUMBER:
                return ignite_type::NUMBER;
            case BOOLEAN:
                return ignite_type::BOOLEAN;
            case DURATION:
                return ignite_type::DURATION;
            case PERIOD:
                return ignite_type::PERIOD;
            default:
                throw ignite_error("Value is out of range for Ignite type: " + std::to_string(t));
        }
    }

    /**
     * Get the client data type value matching an ignite type.
     *
     * @param t Ignite type.
     * @return Matching client data type.
     */
    static std::int32_t from_ignite_type(ignite_type t) {
        switch (t) {
            case ignite_type::BOOLEAN:
                return BOOLEAN;
            case ignite_type::INT8:
                return INT8;
            case ignite_type::INT16:
                return INT16;
            case ignite_type::INT32:
                return INT32;
            case ignite_type::INT64:
                return INT64;
            case ignite_type::FLOAT:
                return FLOAT;
            case ignite_type::DOUBLE:
                return DOUBLE;
            case ignite_type::DECIMAL:
                return DECIMAL;
            case ignite_type::DATE:
                return DATE;
            case ignite_type::TIME:
                return TIME;
            case ignite_type::DATETIME:
                return DATETIME;
            case ignite_type::TIMESTAMP:
                return TIMESTAMP;
            case ignite_type::UUID:
                return UUID;
            case ignite_type::BITMASK:
                return BITMASK;
            case ignite_type::STRING:
                return STRING;
            case ignite_type::BYTE_ARRAY:
                return BYTES;
            case ignite_type::PERIOD:
                return PERIOD;
            case ignite_type::DURATION:
                return DURATION;
            case ignite_type::NUMBER:
                return NUMBER;
            default:
                throw ignite_error("Value is out of range for Ignite type: "
                    + std::to_string(static_cast<std::underlying_type_t<ignite_type>>(t)));
        }
    }
};

} // namespace ignite::detail
