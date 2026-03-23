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

#include "ignite/odbc/query/query.h"

namespace ignite {

/**
 * Special columns query.
 */
class special_columns_query : public query {
public:
    /**
     * Constructor.
     *
     * @param diag Diagnostics collector.
     * @param type Type.
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @param scope Minimum required scope of the rowid.
     * @param nullable Determines whether to return special columns that can have a NULL value.
     */
    special_columns_query(diagnosable_adapter &diag, std::int16_t type, std::string catalog, std::string schema,
        std::string table, std::int16_t scope, std::int16_t nullable);

    /**
     * Destructor.
     */
    ~special_columns_query() override = default;

    /**
     * Execute query.
     *
     * @return True on success.
     */
    sql_result execute() override;

    /**
     * Fetch next result row to application buffers.
     *
     * @param column_bindings Application buffers to put data to.
     * @return Operation result.
     */
    sql_result fetch_next_row(column_binding_map &column_bindings) override;

    /**
     * Get data of the specified column in the result set.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     * @return Operation result.
     */
    sql_result get_column(std::uint16_t column_idx, application_data_buffer &buffer) override;

    /**
     * Close query.
     *
     * @return True on success.
     */
    sql_result close() override;

    /**
     * Get column metadata.
     *
     * @return Column metadata.
     */
    const protocol::column_meta_vector *get_meta() override { return &m_columns_meta; }

    /**
     * Check if data is available.
     *
     * @return True if data is available.
     */
    bool is_data_available() const override { return false; }

    /**
     * Get the number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    std::int64_t affected_rows() const override { return 0; }

    /**
     * Move to the next result set.
     *
     * @return Operation result.
     */
    sql_result next_result_set() override { return sql_result::AI_NO_DATA; }

private:
    /** Query type. */
    std::int16_t m_type;

    /** Catalog name. */
    std::string m_catalog;

    /** Schema name. */
    std::string m_schema;

    /** Table name. */
    std::string m_table;

    /** Minimum required scope of the rowid. */
    std::int16_t m_scope;

    /** Determines whether to return special columns that can have a NULL value. */
    std::int16_t m_nullable;

    /** Query executed. */
    bool m_executed = false;

    /** Columns metadata. */
    protocol::column_meta_vector m_columns_meta;
};

} // namespace ignite
