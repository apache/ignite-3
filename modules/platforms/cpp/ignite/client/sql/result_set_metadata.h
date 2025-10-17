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

#include "ignite/client/sql/column_metadata.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace ignite {

/**
 * @brief SQL result set metadata.
 */
class result_set_metadata {
public:
    // Default
    result_set_metadata() = default;

    /**
     * Constructor.
     *
     * @param columns Columns.
     */
    result_set_metadata(std::vector<column_metadata> columns)
        : m_columns(std::move(columns)) {}

    /**
     * Gets the columns in the same order as they appear in the result set data.
     *
     * @return The columns metadata.
     */
    [[nodiscard]] const std::vector<column_metadata> &columns() const { return m_columns; }

    /**
     * Gets the index of the specified column, or -1 when there is no column
     * with the specified name.
     *
     * @param name The column name.
     * @return Column index.
     */
    [[nodiscard]] std::int32_t index_of(const std::string &name) const {
        if (m_indices.empty()) {
            for (size_t i = 0; i < m_columns.size(); ++i) {
                m_indices[m_columns[i].name()] = i;
            }
        }

        auto it = m_indices.find(name);
        if (it == m_indices.end())
            return -1;
        return std::int32_t(it->second);
    }

private:
    /** Columns metadata. */
    std::vector<column_metadata> m_columns;

    /** Indices of the columns corresponding to their names. */
    mutable std::unordered_map<std::string, size_t> m_indices;
};

} // namespace ignite
