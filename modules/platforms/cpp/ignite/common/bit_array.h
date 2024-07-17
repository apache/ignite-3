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

#include "bytes_view.h"
#include "ignite_error.h"

#include <climits>
#include <cstdint>
#include <vector>

namespace ignite {

/**
 * @brief Bit array.
 */
class bit_array {
public:
    /**
     * Default constructor.
     */
    constexpr bit_array() noexcept = default;

    /**
     * Construct a new bit_array from raw data.
     *
     * @param data Data.
     * @param size Size in bits.
     */
    bit_array(bytes_view data, std::int32_t size)
        : m_size(size)
        , m_data(data.begin(), data.end()) {}

    /**
     * Construct a new bit_array from raw data.
     * All bits are considered valid.
     *
     * @param data Data.
     */
    explicit bit_array(bytes_view data)
        : m_size(std::int32_t(data.size() * CHAR_BIT))
        , m_data(data.begin(), data.end()) {}

    /**
     * Construct a new bit_array of specified size with all bits set to @c value.
     *
     * @param size Size in bits.
     * @param value All bits value.
     */
    explicit bit_array(std::int32_t size, bool value = false)
        : m_size(size)
        , m_data((size + (CHAR_BIT - 1)) / CHAR_BIT, value ? std::byte(0xFF) : std::byte()) {}

    /**
     * Tests a specified bit.
     *
     * @param index Bit index.
     * @return Value of the bit.
     */
    [[nodiscard]] bool test(std::int32_t index) const {
        check_index(index);
        std::size_t byte_index = index / CHAR_BIT;
        std::size_t bit_index = index % CHAR_BIT;
        return int(m_data[byte_index]) & (1 << bit_index);
    }

    /**
     * Sets specified bit to the passed value.
     *
     * @param index Bit index.
     * @param value Value to set.
     */
    void set(std::int32_t index, bool value) {
        check_index(index);
        std::size_t byte_index = index / CHAR_BIT;
        std::size_t bit_index = index % CHAR_BIT;
        if (value) {
            m_data[byte_index] |= std::byte(1 << bit_index);
        } else {
            m_data[byte_index] &= std::byte(~(1 << bit_index));
        }
    }

    /**
     * Gets the raw data.
     */
    [[nodiscard]] constexpr const std::vector<std::byte> &get_raw() const noexcept { return m_data; }

    /**
     * Gets the size in bits.
     */
    [[nodiscard]] constexpr std::int32_t get_size() const noexcept { return m_size; }

    /**
     * Check whether the bitset is empty.
     */
    [[nodiscard]] constexpr bool is_empty() const noexcept { return m_size == 0; }

    /**
     * @brief Comparison operator.
     *
     * @param lhs First value.
     * @param rhs Second value.
     * @return true If values are equal.
     */
    friend constexpr bool operator==(const bit_array &lhs, const bit_array &rhs) noexcept {
        return lhs.m_size == rhs.m_size && lhs.m_data == rhs.m_data;
    }

    /**
     * @brief Comparison operator.
     *
     * @param lhs First value.
     * @param rhs Second value.
     * @return true If values are not equal.
     */
    friend constexpr bool operator!=(const bit_array &lhs, const bit_array &rhs) noexcept { return !(lhs == rhs); }

private:
    /**
     * Check that index is not out of bounds.
     * @param index Index to check.
     */
    void check_index(std::int32_t index) const {
        if (index >= m_size || index < 0) {
            throw ignite_error(
                "Index is out of bounds: index=" + std::to_string(index) + ", size=" + std::to_string(m_size));
        }
    }

    /** Size. */
    std::int32_t m_size = 0;

    /** Data. */
    std::vector<std::byte> m_data{};
};

} // namespace ignite
