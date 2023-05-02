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

#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/cursor.h"
#include "ignite/odbc/query/query.h"

#include <memory>

namespace ignite
{

/** Connection forward-declaration. */
class connection;

/**
 * Query.
 */
class data_query : public query
{
public:
    // Delete
    data_query(data_query &&) = delete;
    data_query(const data_query &) = delete;
    data_query &operator=(data_query &&) = delete;
    data_query &operator=(const data_query &) = delete;

    /**
     * Constructor.
     *
     * @param diag Diagnostics collector.
     * @param connection Associated connection.
     * @param sql SQL query string.
     * @param params SQL params.
     * @param timeout Timeout.
     */
    data_query(diagnosable_adapter& diag, connection& connection, std::string sql, const parameter_set& params,
        std::int32_t& timeout)
        : query(diag, query_type::DATA)
        , m_connection(connection)
        , m_sql(std::move(sql))
        , m_params(params)
        , m_timeout(timeout) { }

    /**
     * Destructor.
     */
    virtual ~data_query();

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
    const column_meta_vector* get_meta() override;

    /**
     * Fetch next result row to application buffers.
     *
     * @param column_bindings Application buffers to put data to.
     * @return Operation result.
     */
    sql_result fetch_next_row(column_binding_map& column_bindings) override;

    /**
     * Get data of the specified column in the result set.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     * @return Operation result.
     */
    sql_result get_column(uint16_t column_idx, application_data_buffer& buffer) override;

    /**
     * Close query.
     *
     * @return Result.
     */
    sql_result close() override;

    /**
     * Check if data is available.
     *
     * @return True if data is available.
     */
    [[nodiscard]] bool is_data_available() const override;

    /**
     * Get number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    [[nodiscard]] std::std::int64_t affected_rows() const override;

    /**
     * Move to the next result set.
     *
     * @return Operation result.
     */
    sql_result next_result_set() override;

    /**
     * Get SQL query string.
     *
     * @return SQL query string.
     */
    [[nodiscard]] const std::string& get_sql() const
    {
        return m_sql;
    }

private:

    /**
     * Check whether all cursors are closed remotely.
     *
     * @return true, if all cursors closed remotely.
     */
    [[nodiscard]] bool is_closed_remotely() const;

    /**
     * Make query prepare request and use response to set internal
     * state.
     *
     * @return Result.
     */
    sql_result make_request_prepare();

    /**
     * Make query execute request and use response to set internal
     * state.
     *
     * @return Result.
     */
    sql_result make_request_execute();

    /**
     * Make query close request.
     *
     * @return Result.
     */
    sql_result make_request_close();

    /**
     * Make data fetch request and use response to set internal state.
     *
     * @return Result.
     */
    sql_result make_request_fetch();

    /**
     * Make next result set request and use response to set internal state.
     *
     * @return Result.
     */
    sql_result make_request_more_results();

    /**
     * Make result set metadata request.
     *
     * @return Result.
     */
    sql_result make_request_resultset_meta();

    /**
     * Process column conversion operation result.
     *
     * @param convRes Conversion result.
     * @param rowIdx Row index.
     * @param column_idx Column index.
     * @return General SQL result.
     */
    sql_result process_conversion_result(conversion_result convRes, std::int32_t rowIdx,
        std::int32_t column_idx);;

    /**
     * Process column conversion operation result.
     *
     * @param convRes Conversion result.
     * @param rowIdx Row index.
     * @param column_idx Column index.
     * @return General SQL result.
     */
    void set_resultset_meta(const column_meta_vector& value);

    /**
     * Close query.
     *
     * @return Result.
     */
    sql_result internal_close();

    /** Connection associated with the statement. */
    connection& m_connection;

    /** SQL Query. */
    std::string m_sql;

    /** parameter bindings. */
    const parameter_set& m_params;

    /** Result set metadata is available */
    bool m_result_meta_available{false};

    /** Result set metadata. */
    column_meta_vector m_result_meta;

    /** Cursor. */
    std::unique_ptr<Cursor> m_cursor;

    /** Number of rows affected. */
    std::vector<std::int64_t> m_rows_affected;

    /** Rows affected index. */
    size_t m_rows_affected_idx{0};

    /** Cached next result page. */
    std::unique_ptr<ResultPage> m_cached_next_page;

    /** Timeout. */
    std::int32_t& m_timeout;
};

} // namespace ignite
