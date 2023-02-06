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
            || std::is_same_v<T, big_integer>)
        {
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
    typedef std::variant<bool, std::int8_t, std::int16_t, std::int32_t, std::int64_t, float, double, big_decimal,
        unsupported_type, // Date
        unsupported_type, // Time
        unsupported_type, // Datetime
        unsupported_type, // Timestamp
        uuid,
        unsupported_type, // Bitmask
        std::string, std::vector<std::byte>,
        unsupported_type, // Period
        unsupported_type, // Duration
        big_integer
        >
        value_type;

    /** Value. */
    value_type m_value;
};

} // namespace ignite
