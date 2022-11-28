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

#include "ignite/client/sql/result_set_metadata.h"
#include "ignite/client/detail/node_connection.h"

#include <cstdint>

namespace ignite::detail {

/**
 * Query result set.
 */
class result_set_impl {
public:
    // Default
    result_set_impl() = default;

    /**
     * Constructor.
     *
     * @param connection Node connection.
     * @param data Row set data.
     */
    result_set_impl(std::shared_ptr<node_connection> connection, bytes_view data)
        : m_connection(std::move(connection)) {

    }

    /**
     * Gets metadata.
     *
     * @return Metadata.
     */
    [[nodiscard]] const result_set_metadata& metadata() const {
        return m_meta;
    }

    /**
     * Gets a value indicating whether this result set contains a collection of rows.
     *
     * @return A value indicating whether this result set contains a collection of rows.
     */
    [[nodiscard]] bool has_rowset() const {
        return m_has_rowset;
    }

    /**
     * Gets the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.), or 0 if
     * the statement returns nothing (such as "ALTER TABLE", etc), or -1 if not applicable.
     *
     * @return The number of rows affected by the DML statement execution.
     */
    [[nodiscard]] std::int64_t affected_rows() const {
        return m_affected_rows;
    }

    /**
     * Gets a value indicating whether a conditional query (such as "CREATE TABLE IF NOT EXISTS") was applied
     * successfully.
     *
     * @return A value indicating whether a conditional query was applied successfully.
     */
    [[nodiscard]] bool was_applied() const {
        return m_was_applied;
    }

private:
    /** Result set metadata. */
    result_set_metadata m_meta;

    /** Has row set. */
    bool m_has_rowset{false};

    /** Affected rows. */
    std::int64_t m_affected_rows{-1};

    /** Statement was applied. */
    bool m_was_applied{false};

    /** Connection. */
    std::shared_ptr<node_connection> m_connection;
};

} // namespace ignite::detail
