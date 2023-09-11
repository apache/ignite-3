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

#include "ignite/network/data_buffer.h"
#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/common_types.h"
#include "ignite/protocol/reader.h"

#include <cstdint>

namespace ignite {
/**
 * Query result page.
 */
class result_page {
    enum { DEFAULT_ALLOCATED_MEMORY = 1024 };

public:
    // Delete
    result_page() = delete;
    result_page(result_page &&) = delete;
    result_page(const result_page &) = delete;
    result_page &operator=(result_page &&) = delete;
    result_page &operator=(const result_page &) = delete;

    /**
     * Constructor.
     *
     * @param data Page data.
     * @param reader Page reader.
     */
    result_page(network::data_buffer_owning &&data, std::unique_ptr<protocol::reader> &&reader)
        : m_data(std::move(data))
        , m_reader(std::move(reader)) {
        m_size = m_reader->read_int32();
    }

    /**
     * Get page size.
     * @return Page size.
     */
    [[nodiscard]] std::uint32_t get_size() const { return m_size; }

    /**
     * Get page data.
     *
     * @return Page data.
     */
    network::data_buffer_owning &get_data() { return m_data; }

    /**
     * Get row.
     * @param idx Row index.
     * @return Row data.
     */
    bytes_view get_row(std::uint32_t idx) {
        assert(idx < m_size);

        return m_reader->read_binary();
    }

private:
    /** Page size in rows. */
    std::uint32_t m_size{0};

    /** Memory that contains current row page data. */
    network::data_buffer_owning m_data;

    /** Reader that reads current memory. */
    std::unique_ptr<protocol::reader> m_reader;
};

} // namespace ignite