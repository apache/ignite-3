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

#include "ignite_error.h"

#include <vector>
#include <cstdint>

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
    bit_array(std::vector<uint8_t> data, std::int32_t size)
        : m_size(size)
        , m_data(std::move(data)) { }

    /**
     * Construct a new bit_array of specified size with all bits set to @c value.
     *
     * @param size Size in bits.
     * @param value All bits value.
     */
    bit_array(std::int32_t size, bool value = false)
        : m_size(size)
        , m_data((size + 7) / 8, value ? 0xFF : 0) { }

    /**
     * Tests a specified bit.
     *
     * @param index Bit index.
     * @return Value of the bit.
     */
    [[nodiscard]] bool test(std::int32_t index) const {
        check_index(index);
        std::size_t byte_index = index / 8;
        std::size_t bit_index = index % 8;
        return m_data[byte_index] & (1 << bit_index);
    }

    /**
     * Sets specified bit to the passed value.
     *
     * @param index Bit index.
     * @param value Value to set.
     */
    void set(std::int32_t index, bool value) {
        check_index(index);
        std::size_t byte_index = index / 8;
        std::size_t bit_index = index % 8;
        if (value)
            m_data[byte_index] |= 1 << bit_index;
        else
            m_data[byte_index] &= ~(1 << bit_index);
    }

    /**
     * Gets the raw data.
     */
    [[nodiscard]] constexpr const std::vector<uint8_t>& get_raw() const noexcept { return m_data; }

    /**
     * Gets the size in bits.
     */
    [[nodiscard]] constexpr std::int32_t get_size() const noexcept { return m_size; }

    /**
     * Check whether the bitset is empty.
     */
    [[nodiscard]] constexpr bool is_empty() const noexcept { return m_size == 0; }

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
    std::vector<uint8_t> m_data{};
};


} // namespace ignite
