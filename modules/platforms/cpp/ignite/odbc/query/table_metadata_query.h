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

#include "ignite/odbc/meta/table_meta.h"
#include "ignite/odbc/query/query.h"

namespace ignite {

/** Connection forward-declaration. */
class sql_connection;

/**
 * Table metadata query.
 */
class table_metadata_query : public query {
public:
    /**
     * Constructor.
     *
     * @param diag Diagnostics collector.
     * @param connection Associated connection.
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param table_type Table type search pattern.
     */
    table_metadata_query(diagnosable_adapter &diag, sql_connection &connection, std::string catalog, std::string schema,
        std::string table, std::string table_type);

    /**
     * Destructor.
     */
    ~table_metadata_query() override = default;

    /**
     * Execute query.
     *
     * @return True on success.
     */
    sql_result execute() override;

    /**
     * Get column metadata.
     *
     * @return Column metadata.
     */
    const column_meta_vector *get_meta() override;

    /**
     * Fetch next result row to application buffers.
     *
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
     * Check if data is available.
     *
     * @return True if data is available.
     */
    bool is_data_available() const override;

    /**
     * Get number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    [[nodiscard]] std::int64_t affected_rows() const override;

    /**
     * Move to the next result set.
     *
     * @return Operation result.
     */
    sql_result next_result_set() override;

private:
    /**
     * Make get columns metadata requests and use response to set internal state.
     *
     * @return True on success.
     */
    sql_result make_request_get_tables_meta();

    /** Connection associated with the statement. */
    sql_connection &m_connection;

    /** Catalog search pattern. */
    std::string m_catalog;

    /** Schema search pattern. */
    std::string m_schema;

    /** Table search pattern. */
    std::string m_table;

    /** Table type search pattern. */
    std::string m_table_type;

    /** Query executed. */
    bool m_executed{false};

    /** Fetched flag. */
    bool m_fetched{false};

    /** Has result set. */
    bool m_has_result_set{false};

    /** Fetched metadata. */
    table_meta_vector m_meta;

    /** Metadata cursor. */
    table_meta_vector::iterator m_cursor;

    /** Columns metadata. */
    column_meta_vector m_columns_meta;
};

} // namespace ignite
