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
#include "ignite/odbc/query/query.h"
#include "ignite/odbc/sql_connection.h"

namespace ignite
{

/**
 * Data query.
 */
class data_query : public query
{
public:
    // Delete
    data_query(data_query&&) = delete;
    data_query(const data_query&) = delete;
    data_query& operator=(data_query&&) = delete;
    data_query& operator=(const data_query&) = delete;

    /**
     * Constructor.
     *
     * @param diag Diagnostics collector.
     * @param connection Associated connection.
     * @param sql SQL query string.
     * @param params SQL params.
     * @param timeout Timeout.
     */
    data_query(diagnosable_adapter &diag, sql_connection &connection, std::string sql, const parameter_set &params,
        std::int32_t &timeout);

    /**
     * Destructor.
     */
    virtual ~data_query();

    /**
     * Execute query.
     *
     * @return True on success.
     */
    virtual sql_result execute();

    /**
     * Get column metadata.
     *
     * @return Column metadata.
     */
    virtual const column_meta_vector *get_meta();

    /**
     * Fetch next result row to application buffers.
     *
     * @param column_bindings Application buffers to put data to.
     * @return Operation result.
     */
    virtual sql_result fetch_next_row(column_binding_map &column_bindings);

    /**
     * Get data of the specified column in the result set.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     * @return Operation result.
     */
    virtual sql_result get_column(std::uint16_t column_idx, application_data_buffer &buffer);

    /**
     * Close query.
     *
     * @return Result.
     */
    virtual sql_result close();

    /**
     * Check if data is available.
     *
     * @return True if data is available.
     */
    virtual bool data_available() const;

    /**
     * Get number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    virtual std::int64_t affected_rows() const;

    /**
     * Move to the next result set.
     *
     * @return Operation result.
     */
    virtual sql_result next_result_set();

    /**
     * Get SQL query string.
     *
     * @return SQL query string.
     */
    const std::string& get_sql() const
    {
        return m_sql;
    }

private:
    /**
     * Check whether all cursors are closed remotely.
     *
     * @return true, if all cursors closed remotely.
     */
    bool is_closed_remotely() const;

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
     * @param conv_res Conversion result.
     * @param row_idx Row index.
     * @param column_idx Column index.
     * @return General SQL result.
     */
    sql_result process_conversion_result(conversion_result conv_res, std::int32_t row_idx, std::int32_t column_idx);

    /**
     * Set metadata for the result set.
     *
     * @param value Metadata.
     */
    void set_resultset_meta(const column_meta_vector &value);

    /**
     * Close query.
     *
     * @return Result.
     */
    sql_result internal_close();

    /** Connection associated with the statement. */
    sql_connection &m_connection;

    /** SQL query. */
    std::string m_sql;

    /** Parameter bindings. */
    const parameter_set &m_params;

    /** Result set metadata is available */
    bool m_result_meta_available{false};

    /** Result set metadata. */
    column_meta_vector m_result_meta;

    /** Cursor. */
    std::unique_ptr<cursor> m_cursor;

    /** Number of rows affected. */
    std::vector<std::int64_t> m_rows_affected;

    /** Rows affected index. */
    std::size_t m_rows_affected_idx{0};

    /** Cached next result page. */
    std::unique_ptr<result_page> m_cached_next_page;

    /** Timeout. */
    std::int32_t &m_timeout;
};

} // namespace ignite
