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

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/meta/column_meta.h"

#include <cstdint>
#include <map>

namespace ignite {

/** Query type. */
enum class query_type {
    /** Data query type. */
    DATA,

    /** Batch query type. */
    BATCH,

    /** Table metadata. */
    TABLE_METADATA,

    /** Column metadata. */
    COLUMN_METADATA,

    /** Type info. */
    TYPE_INFO,

    /** Foreign keys. */
    FOREIGN_KEYS,

    /** Primary keys. */
    PRIMARY_KEYS,

    /** Special columns. */
    SPECIAL_COLUMNS,
};

/**
 * Query.
 */
class query {
public:
    /**
     * Virtual destructor
     */
    virtual ~query() = default;

    /**
     * Execute query.
     *
     * @return Execution result.
     */
    virtual sql_result execute() = 0;

    /**
     * Fetch next result row to application buffers.
     *
     * @param column_bindings Application buffers to put data to.
     * @return Operation result.
     */
    virtual sql_result fetch_next_row(column_binding_map &m_column_bindings) = 0;

    /**
     * Get data of the specified column in the result set.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     * @return Operation result.
     */
    virtual sql_result get_column(std::uint16_t column_idx, application_data_buffer &buffer) = 0;

    /**
     * Close query.
     *
     * @return Operation result.
     */
    virtual sql_result close() = 0;

    /**
     * Get column metadata.
     *
     * @return Column metadata.
     */
    [[nodiscard]] virtual const column_meta_vector *get_meta() {
        static const column_meta_vector empty;

        return &empty;
    }

    /**
     * Check if data is available.
     *
     * @return True if data is available.
     */
    [[nodiscard]] virtual bool is_data_available() const = 0;

    /**
     * Get number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    [[nodiscard]] virtual std::int64_t affected_rows() const = 0;

    /**
     * Move to the next result set.
     *
     * @return Operation result.
     */
    virtual sql_result next_result_set() = 0;

    /**
     * Get query type.
     *
     * @return Query type.
     */
    [[nodiscard]] query_type get_type() const { return m_type; }

protected:
    /**
     * Constructor.
     */
    query(diagnosable_adapter &diag, query_type type)
        : m_diag(diag)
        , m_type(type) {}

    /** Diagnostics collector. */
    diagnosable_adapter &m_diag;

    /** Query type. */
    query_type m_type;
};

} // namespace ignite
