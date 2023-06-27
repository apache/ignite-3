/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>

#include <cstdint>

namespace ignite
{

class cursor
{
public:
    // Delete
    cursor(cursor&&) = delete;
    cursor(const cursor&) = delete;
    cursor& operator=(cursor&&) = delete;
    cursor& operator=(const cursor&) = delete;

    /**
     * Constructor.
     * @param query_id ID of the executed query.
     */
    explicit cursor(std::int64_t query_id)
        : m_query_id(query_id) {}

    /**
     * Check if the cursor has data.
     *
     * @return True if the cursor has data.
     */
    [[nodiscard]] bool has_data() const {
        return false;
    }

    /**
     * Check whether cursor closed remotely.
     *
     * @return true, if the cursor closed remotely.
     */
    [[nodiscard]] bool is_closed_remotely() const {
        return false;
    }

    /**
     * Get query ID.
     *
     * @return Query ID.
     */
    [[nodiscard]] std::int64_t get_query_id() const {
        return m_query_id;
    }

private:
    /** Cursor id. */
    std::int64_t m_query_id;
};

}
