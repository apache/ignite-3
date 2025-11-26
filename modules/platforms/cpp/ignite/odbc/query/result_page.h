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
     * @param rows Result rows.
     */
    result_page(network::data_buffer_owning &&data, std::vector<bytes_view> &&rows)
        : m_data(std::move(data))
        , m_rows(std::move(rows)) {}

    /**
     * Get page size.
     *
     * @return Page size.
     */
    [[nodiscard]] std::size_t get_size() const { return m_rows.size(); }

    /**
     * Get page data.
     *
     * @return Page data.
     */
    network::data_buffer_owning &get_data() { return m_data; }

    /**
     * Get the row.
     *
     * @param idx Row index.
     * @return Row data.
     */
    [[nodiscard]] bytes_view get_row(std::uint32_t idx) const {
        return m_rows.at(idx);
    }

private:
    /** Memory that contains current row page data. */
    network::data_buffer_owning m_data;

    /** Rows data. */
    std::vector<bytes_view> m_rows;
};

} // namespace ignite