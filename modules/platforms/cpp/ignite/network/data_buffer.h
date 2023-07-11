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

#include <ignite/common/bytes_view.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>

namespace ignite::network {

/**
 * Non-owning data buffer.
 *
 * Represents a consumable chunk of data. Does not hold data ownership.
 */
class data_buffer_ref {
public:
    // Default
    data_buffer_ref() = default;

    /**
     * Constructor.
     *
     * @param data Data.
     */
    explicit data_buffer_ref(bytes_view data)
        : m_data(data) {}

    /**
     * Constructor.
     *
     * @param data Data.
     * @param pos Start of data.
     * @param len Length.
     */
    data_buffer_ref(bytes_view data, size_t pos, size_t len)
        : m_data(data.substr(pos, len)) {}

    /**
     * Consume buffer data by the vector.
     *
     * @param dst Vector to append data to.
     * @param bytes Number of bytes to consume.
     */
    void consume_by(std::vector<std::byte> &dst, size_t bytes) {
        if (bytes > m_data.size())
            bytes = m_data.size();

        dst.insert(dst.end(), m_data.begin(), m_data.begin() + ptrdiff_t(bytes));
        skip(bytes);
    }

    /**
     * Check whether data buffer was fully consumed.
     *
     * @return @c true if the buffer is empty and @c false otherwise.
     */
    [[nodiscard]] bool empty() const { return m_data.empty(); }

    /**
     * Consume the whole buffer.
     *
     * @return Buffer containing consumed data.
     */
    data_buffer_ref consume_entirely() {
        data_buffer_ref res(*this);
        m_data = {};

        return res;
    }

    /**
     * Skip specified number of bytes.
     *
     * @param bytes Bytes to skip.
     */
    void skip(size_t bytes) {
        if (bytes >= m_data.size())
            m_data = {};
        else
            m_data.remove_prefix(bytes);
    }

    /**
     * Get bytes view.
     *
     * @return Bytes view.
     */
    [[nodiscard]] bytes_view get_bytes_view() const { return m_data; }

private:
    /** Data. */
    bytes_view m_data;
};

/**
 * Owning data buffer.
 *
 * Represents a consumable chunk of data. Holds data ownership.
 */
class data_buffer_owning {
public:
    // Default.
    data_buffer_owning() = default;

    /**
     * Constructor.
     *
     * @param data Data.
     * @param pos Position.
     */
    explicit data_buffer_owning(std::vector<std::byte> &&data, size_t pos = 0)
        : m_memory(std::move(data))
        , m_pos(pos) {}

    /**
     * Consume buffer data by the vector.
     *
     * @param dst Vector to append data to.
     * @param bytes Number of bytes to consume.
     */
    void consume_by(std::vector<std::byte> &dst, size_t bytes) {
        bytes = std::min(m_memory.size() - m_pos, bytes);

        dst.insert(dst.end(), m_memory.data() + m_pos, m_memory.data() + m_pos + bytes);
        skip(bytes);
    }

    /**
     * Check whether data buffer was fully consumed.
     *
     * @return @c true if the buffer is empty and @c false otherwise.
     */
    [[nodiscard]] bool empty() const { return size() == 0; }

    /**
     * Consume the whole buffer.
     *
     * @return Buffer containing consumed data.
     */
    data_buffer_owning consume_entirely() {
        data_buffer_owning copy(*this);
        skip(size());
        return copy;
    }

    /**
     * Skip specified number of bytes.
     *
     * @param bytes Bytes to skip.
     */
    void skip(size_t bytes) { m_pos += std::min(bytes, size()); }

    /**
     * Get bytes view.
     *
     * @return Bytes view.
     */
    [[nodiscard]] bytes_view get_bytes_view() const { return {m_memory.data() + m_pos, size()}; }

    /**
     * Convert to underlying data.
     *
     * @return Data vector.
     */
    [[nodiscard]] std::vector<std::byte> extract_data() && {
        if (m_pos) {
            m_memory.erase(m_memory.begin(), m_memory.begin() + ptrdiff_t(m_pos));
            m_pos = 0;
        }
        return std::move(m_memory);
    }

private:
    /**
     * Get size.
     *
     * @return Size.
     */
    [[nodiscard]] size_t size() const {
        assert(m_memory.size() >= m_pos);
        return m_memory.size() - m_pos;
    }

    /** Memory. */
    std::vector<std::byte> m_memory;

    /** Data. */
    size_t m_pos{0};
};

} // namespace ignite::network
