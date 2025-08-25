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
#include "ignite/odbc/sql_connection.h"

namespace ignite {

/**
 * Foreign keys query.
 */
class foreign_keys_query : public query {
public:
    /**
     * Constructor.
     *
     * @param diag Diagnostics collector.
     * @param primary_catalog Primary key catalog name.
     * @param primary_schema Primary key schema name.
     * @param primary_table Primary key table name.
     * @param foreign_catalog Foreign key catalog name.
     * @param foreign_schema Foreign key schema name.
     * @param foreign_table Foreign key table name.
     */
    foreign_keys_query(diagnosable_adapter &diag, std::string primary_catalog, std::string primary_schema,
        std::string primary_table, std::string foreign_catalog, std::string foreign_schema, std::string foreign_table);

    /**
     * Destructor.
     */
    ~foreign_keys_query() override = default;

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
    const protocol::column_meta_vector *get_meta() override { return &m_columns_meta; }

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
    bool is_data_available() const override { return false; }

    /**
     * Get the number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    [[nodiscard]] std::int64_t affected_rows() const override { return 0; }

    /**
     * Move to the next result set.
     *
     * @return Operation result.
     */
    sql_result next_result_set() override { return sql_result::AI_NO_DATA; }

private:
    /** Primary key catalog name. */
    std::string m_primary_catalog;

    /** Primary key schema name. */
    std::string m_primary_schema;

    /** Primary key table name. */
    std::string m_primary_table;

    /** Foreign key catalog name. */
    std::string m_foreign_catalog;

    /** Foreign key schema name. */
    std::string m_foreign_schema;

    /** Foreign key table name. */
    std::string m_foreign_table;

    /** Query executed. */
    bool m_executed{false};

    /** Columns metadata. */
    protocol::column_meta_vector m_columns_meta;
};

} // namespace ignite
