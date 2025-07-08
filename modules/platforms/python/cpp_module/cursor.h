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

#include "ignite/protocol/sql/column_meta.h"
#include "result_page.h"

#include <memory>

#include <cstdint>

/**
 * Cursor.
 *
 * TODO: https://issues.apache.org/jira/browse/IGNITE-25744 Probably needs to be moved to the protocol library.
 */
class cursor {
public:
    // Delete
    cursor(cursor &&) = delete;
    cursor(const cursor &) = delete;
    cursor &operator=(cursor &&) = delete;
    cursor &operator=(const cursor &) = delete;

    /**
     * Constructor.
     *
     * @param page Data page.
     */
    explicit cursor(std::unique_ptr<result_page> page)
        : m_current_page(std::move(page)) {}

    /**
     * Move the cursor to the next row.
     *
     * @return False if data update required or no more data.
     */
    bool next(const ignite::protocol::column_meta_vector &columns) {
        if (!has_data())
            return false;

        ++m_page_pos;
        if (std::uint32_t(m_page_pos) >= m_current_page->get_size()) {
            m_current_page.reset();
            return false;
        }

        ++m_result_set_pos;
        auto row_data = m_current_page->get_row(m_page_pos);

        auto columns_cnt = columns.size();
        ignite::binary_tuple_parser parser(std::int32_t(columns_cnt), row_data);

        m_row.clear();
        for (size_t i = 0; i < columns_cnt; ++i) {
            auto &column = columns[i];
            m_row.push_back(ignite::protocol::read_next_column(parser, column.get_data_type(), column.get_scale()));
        }

        return true;
    }

    /**
     * Check if the cursor has data.
     *
     * @return True if the cursor has data.
     */
    [[nodiscard]] bool has_data() const { return bool(m_current_page); }

    /**
     * Update current cursor page data.
     *
     * @param new_page New result page.
     */
    void update_data(std::unique_ptr<result_page> new_page) {
        m_current_page = std::move(new_page);

        m_page_pos = -1;

        m_row.clear();
    }

    /**
     * Get the current row.
     *
     * @return  Row.
     */
    [[nodiscard]] const std::vector<ignite::primitive> &get_row() const { return m_row; }

    /**
     * Get the current position in the result set.
     *
     * @return Current position in the result set.
     */
    [[nodiscard]] std::int32_t get_result_set_pos() const { return m_result_set_pos; }

private:
    /** Current page. */
    std::unique_ptr<result_page> m_current_page;

    /** Row position in current page. */
    std::int32_t m_page_pos{-1};

    /** Row position in the result set. */
    std::int32_t m_result_set_pos{0};

    /** Current row. */
    std::vector<ignite::primitive> m_row;
};
