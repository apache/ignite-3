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

#include "big_decimal.h"
#include "big_integer.h"
#include "bit_array.h"
#include "ignite_date.h"
#include "ignite_date_time.h"
#include "ignite_duration.h"
#include "ignite_error.h"
#include "ignite_period.h"
#include "ignite_time.h"
#include "ignite_timestamp.h"
#include "ignite_type.h"
#include "uuid.h"

#include <cstdint>
#include <optional>
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
     * Null constructor.
     */
    primitive(std::nullptr_t) {} // NOLINT(google-explicit-constructor)

    /**
     * Null option constructor.
     */
    primitive(std::nullopt_t) {} // NOLINT(google-explicit-constructor)

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
     * Constructor for string value.
     *
     * @param value Value.
     */
    primitive(std::string_view value) // NOLINT(google-explicit-constructor)
        : m_value(std::string(value)) {}

    /**
     * Constructor for string value.
     *
     * @param value Value.
     */
    primitive(const char *value) // NOLINT(google-explicit-constructor)
        : m_value(std::string(value)) {}

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
     * Constructor for period value.
     *
     * @param value Value.
     */
    primitive(ignite_period value) // NOLINT(google-explicit-constructor)
        : m_value(value) {}

    /**
     * Constructor for duration value.
     *
     * @param value Value.
     */
    primitive(ignite_duration value) // NOLINT(google-explicit-constructor)
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
        if constexpr (std::is_same_v<T, bool> // Bool
            || std::is_same_v<T, std::int8_t> // Int8
            || std::is_same_v<T, std::int16_t> // Int16
            || std::is_same_v<T, std::int32_t> // Int32
            || std::is_same_v<T, std::int64_t> // Int64
            || std::is_same_v<T, float> // Float
            || std::is_same_v<T, double> // Double
            || std::is_same_v<T, uuid> // Uuid
            || std::is_same_v<T, std::string> // String
            || std::is_same_v<T, std::vector<std::byte>> // Bytes
            || std::is_same_v<T, big_decimal> // Decimal
            || std::is_same_v<T, big_integer> // Number
            || std::is_same_v<T, ignite_date> // Date
            || std::is_same_v<T, ignite_date_time> // DateTime
            || std::is_same_v<T, ignite_time> // Time
            || std::is_same_v<T, ignite_timestamp> // Timestamp
            || std::is_same_v<T, ignite_period> // Period
            || std::is_same_v<T, ignite_duration> // Duration
            || std::is_same_v<T, bit_array> // Bit Array
        ) {
            return std::get<T>(m_value);
        } else {
            static_assert(sizeof(T) == 0, "Type is not an Ignite primitive type or is not yet supported");
        }
    }

    /**
     * Check whether element is null.
     *
     * @return Value indicating whether element is null.
     */
    [[nodiscard]] bool is_null() const noexcept { return m_value.index() == 0; }

    /**
     * Get primitive type.
     *
     * @return Primitive type.
     */
    [[nodiscard]] ignite_type get_type() const noexcept { return static_cast<ignite_type>(m_value.index()); }

    /**
     * @brief Comparison operator.
     *
     * @param lhs First value.
     * @param rhs Second value.
     * @return true If values are equal.
     */
    friend constexpr bool operator==(const primitive &lhs, const primitive &rhs) noexcept {
        return lhs.m_value == rhs.m_value;
    }

    /**
     * @brief Comparison operator.
     *
     * @param lhs First value.
     * @param rhs Second value.
     * @return true If values are not equal.
     */
    friend constexpr bool operator!=(const primitive &lhs, const primitive &rhs) noexcept {
        return lhs.m_value != rhs.m_value;
    }

private:
    /** Value type. */
    typedef std::variant<std::nullptr_t, // Null = 0
        bool, // Bool = 1
        std::int8_t, // Int8 = 2
        std::int16_t, // Int16 = 3
        std::int32_t, // Int32 = 4
        std::int64_t, // Int64 = 5
        float, // Float = 6
        double, // Double = 7
        big_decimal, // Decimal = 8
        ignite_date, // Date = 9
        ignite_time, // Time = 10
        ignite_date_time, // Datetime = 11
        ignite_timestamp, // Timestamp = 12
        uuid, // UUID = 13
        bit_array, // Bitmask = 14
        std::string, // String = 15
        std::vector<std::byte>, // Bytes = 16
        ignite_period, // Period = 17
        ignite_duration, // Duration = 18
        big_integer // Big Integer = 19
        >
        value_type;

    /** Value. */
    value_type m_value;
};

} // namespace ignite
