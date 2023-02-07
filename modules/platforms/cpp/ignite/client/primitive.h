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

#include "ignite/client/sql/sql_column_type.h"
#include "ignite/common/ignite_error.h"
#include "ignite/common/uuid.h"
#include "ignite/common/big_decimal.h"
#include "ignite/common/big_integer.h"
#include "ignite/common/ignite_date.h"
#include "ignite/common/ignite_date_time.h"
#include "ignite/common/ignite_time.h"
#include "ignite/common/ignite_timestamp.h"
#include "ignite/common/bit_array.h"

#include <cstdint>
#include <type_traits>
#include <variant>
#include <vector>

namespace ignite {

/**
 * Ignite primitive type.
 */
class primitive {
public:
    // Default
    primitive() = default;

    /**
     * Constructor for boolean value.
     *
     * @param value Value.
     */
    primitive(bool value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for std::int8_t value.
     *
     * @param value Value.
     */
    primitive(std::int8_t value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for std::int16_t value.
     *
     * @param value Value.
     */
    primitive(std::int16_t value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for std::int32_t value.
     *
     * @param value Value.
     */
    primitive(std::int32_t value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for std::int64_t value.
     *
     * @param value Value.
     */
    primitive(std::int64_t value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for float value.
     *
     * @param value Value.
     */
    primitive(float value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for double value.
     *
     * @param value Value.
     */
    primitive(double value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for UUID value.
     *
     * @param value Value.
     */
    primitive(uuid value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for string value.
     *
     * @param value Value.
     */
    primitive(std::string value) // NOLINT(google-explicit-constructor)
        : m_value(std::move(value)) {}

    /**
     * Constructor for byte array value.
     *
     * @param value Value.
     */
    primitive(std::vector<std::byte> value) // NOLINT(google-explicit-constructor)
        : m_value(std::move(value)) {}

    /**
     * Constructor for byte array value.
     *
     * @param buf Buffer.
     * @param len Buffer length.
     */
    primitive(std::byte *buf, std::size_t len)
        : m_value(std::vector<std::byte>(buf, buf + len)) {}

    /**
     * Constructor for big decimal value.
     *
     * @param value Value.
     */
    primitive(big_decimal value) // NOLINT(google-explicit-constructor)
        : m_value(std::move(value)) {}

    /**
     * Constructor for big integer value.
     *
     * @param value Value.
     */
    primitive(big_integer value) // NOLINT(google-explicit-constructor)
        : m_value(std::move(value)) {}

    /**
     * Constructor for date value.
     *
     * @param value Value.
     */
    primitive(ignite_date value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for date-time value.
     *
     * @param value Value.
     */
    primitive(ignite_date_time value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for time value.
     *
     * @param value Value.
     */
    primitive(ignite_time value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for timestamp value.
     *
     * @param value Value.
     */
    primitive(ignite_timestamp value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for bitmask value.
     *
     * @param value Value.
     */
    primitive(bit_array value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Get underlying value.
     *
     * @tparam T Type of value to try and get.
     * @return Value of the specified type.
     * @throw ignite_error if primitive contains value of any other type.
     */
    template<typename T>
    [[nodiscard]] const T &get() const {
        if constexpr (
            std::is_same_v< T, bool>
            || std::is_same_v<T, std::int8_t>
            || std::is_same_v<T, std::int16_t>
            || std::is_same_v<T, std::int32_t>
            || std::is_same_v<T, std::int64_t>
            || std::is_same_v<T, float>
            || std::is_same_v<T, double>
            || std::is_same_v<T, uuid>
            || std::is_same_v<T, std::string>
            || std::is_same_v<T, std::vector<std::byte>>
            || std::is_same_v<T, big_decimal>
            || std::is_same_v<T, big_integer>
            || std::is_same_v<T, ignite_date>
            || std::is_same_v<T, ignite_date_time>
            || std::is_same_v<T, ignite_time>
            || std::is_same_v<T, ignite_timestamp>
            || std::is_same_v<T, bit_array>
        ) {
            return std::get<T>(m_value);
        } else {
            static_assert(sizeof(T) == 0, "Type is not an Ignite primitive type or is not yet supported");
        }
    }

    /**
     * Get primitive type.
     *
     * @return Primitive type.
     */
    [[nodiscard]] column_type get_type() const {
        // TODO: Ensure by tests
        return static_cast<column_type>(m_value.index());
    }

private:
    /** Unsupported type. */
    typedef void *unsupported_type;

    /** Value type. */
    typedef std::variant<
        bool,                   // Bool = 0
        std::int8_t,            // Int8 = 1
        std::int16_t,           // Int16 = 2
        std::int32_t,           // Int32 = 3
        std::int64_t,           // Int64 = 4
        float,                  // Float = 5
        double,                 // Double = 6
        big_decimal,            // Decimal = 7
        ignite_date,            // Date = 8
        ignite_time,            // Time = 9
        ignite_date_time,       // Datetime = 10
        ignite_timestamp,       // Timestamp = 11
        uuid,                   // UUID = 12
        bit_array,              // Bitmask = 13
        std::string,            // String = 14
        std::vector<std::byte>, // Bytes = 15
        unsupported_type,       // Period = 16
        unsupported_type,       // Duration = 17
        big_integer             // Big Integer = 18
    > value_type;

    /** Value. */
    value_type m_value;
};

} // namespace ignite
