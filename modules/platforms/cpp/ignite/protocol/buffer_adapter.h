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

#include <limits>

namespace ignite::protocol {

/**
 * Buffer adapter.
 *
 * Used to allow msgpack classes to write data to std::vector<std::byte>.
 */
class buffer_adapter {
public:
    /** Length header size in bytes. */
    static constexpr size_t LENGTH_HEADER_SIZE = 4;

    /**
     * Constructor.
     *
     * @param data Data.
     */
    explicit buffer_adapter(std::vector<std::byte> &data)
        : m_buffer(data)
        , m_length_pos(std::numeric_limits<std::size_t>::max()) {}

    /**
     * Write raw data.
     *
     * @param data Data to write.
     */
    void write_raw(bytes_view data) { m_buffer.insert(m_buffer.end(), data.begin(), data.end()); }

    /**
     * Get underlying data buffer view.
     *
     * @return Underlying data buffer view.
     */
    [[nodiscard]] bytes_view data() const { return m_buffer; }

    /**
     * Reserving space for length header.
     */
    void reserve_length_header() {
        m_length_pos = m_buffer.size();
        m_buffer.insert(m_buffer.end(), LENGTH_HEADER_SIZE, std::byte{0});
    }

    /**
     * Write buffer length to previously reserved position.
     */
    void write_length_header();

private:
    /** Buffer */
    std::vector<std::byte> &m_buffer;

    /** Length position. */
    std::size_t m_length_pos{std::numeric_limits<std::size_t>::max()};
};

} // namespace ignite::protocol
