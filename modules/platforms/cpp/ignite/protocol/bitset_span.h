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

#include "ignite/common/bytes_view.h"

#include <cassert>
#include <climits>
#include <cstddef>
#include <cstring>

namespace ignite::protocol {

/**
 * Bitset span.
 */
class bitset_span {
public:
    /**
     * Constructor.
     *
     * @param begin Beginning of the span.
     * @param size Size in bytes.
     */
    bitset_span(std::byte *begin, std::size_t size)
        : m_data(begin)
        , m_size(size) {}

    /**
     * Constructor.
     *
     * @param mask Mask bytes.
     */
    bitset_span(std::vector<std::byte> mask)
        : m_data(mask.data())
        , m_size(mask.size()) {}

    /**
     * Reset bitset.
     */
    void reset() {
        std::memset(m_data, 0, m_size);
    }

    /**
     * Test specified bit.
     *
     * @param bit_idx Bit index.
     */
    bool test(std::size_t bit_idx) const {
        std::size_t byte_idx = bit_idx / CHAR_BIT;
        assert(byte_idx < m_size);
        return (m_data[byte_idx] & std::byte(1 << (bit_idx % CHAR_BIT))) != std::byte{0};
    }

    /**
     * Set specified bit.
     *
     * @param bit_idx Bit index.
     */
    void set(std::size_t bit_idx) {
        std::size_t byte_idx = bit_idx / CHAR_BIT;
        assert(byte_idx < m_size);
        m_data[byte_idx] |= std::byte(1 << (bit_idx % CHAR_BIT));
    }

    /**
     * Get data.
     *
     * @return Data view.
     */
    [[nodiscard]] bytes_view data() const { return {m_data, m_size}; }

private:
    /** A pointer to the beginning of the byte span. */
    std::byte *m_data;

    /** Length of the span in bytes. */
    std::size_t m_size;
};

} // namespace ignite::protocol
