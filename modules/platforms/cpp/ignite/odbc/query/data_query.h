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
#include "ignite/odbc/query/cursor.h"
#include "ignite/odbc/query/query.h"
#include "ignite/odbc/query/result_page.h"
#include "ignite/odbc/sql_connection.h"

namespace ignite {

/**
 * SQL parameter.
 */
struct sql_parameter {
    bool nullable;
    ignite_type data_type;
    std::int32_t scale;
    std::int32_t precision;
};

/**
 * Data query.
 */
class data_query : public query {
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
    data_query(diagnosable_adapter &diag, sql_connection &connection, std::string sql, const parameter_set &params,
        std::int32_t &timeout);

    /**
     * Destructor.
     */
    ~data_query() override;

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
    [[nodiscard]] std::int64_t affected_rows() const override;

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
    [[nodiscard]] const std::string &get_query() const { return m_query; }

    /**
     * Make result set metadata request.
     *
     * @return Result.
     */
    sql_result update_meta();

    /**
     * Get the parameter by index.
     *
     * @param idx Parameter index.
     * @return Parameter.
     */
    [[nodiscard]] const sql_parameter *get_sql_param(std::int16_t idx);

    /**
     * Get expected parameter number.
     * Using metadata. If metadata was not updated returns zero.
     *
     * @return Expected parameters number.
     */
    [[nodiscard]] std::size_t get_expected_param_num() const { return m_params_meta.size(); }

    /**
     * Check if parameters meta is available.
     *
     * @return @c true if available.
     */
    [[nodiscard]] bool is_param_meta_available() const { return m_params_meta_available; }

private:
    /**
     * Check whether all cursors are closed remotely.
     *
     * @return true, if all cursors closed remotely.
     */
    [[nodiscard]] bool is_closed_remotely() const { return !has_more_pages(); }

    /**
     * Check if there are more data pages locally or on server.
     *
     * @return @c true, if there is more data pages.
     */
    [[nodiscard]] bool has_more_pages() const { return m_has_more_pages; }

    /**
     * Check if there are more data pages locally or on server.
     *
     * @return @c true, if there is more data pages.
     */
    [[nodiscard]] bool has_more_rows() const { return has_more_pages() || (m_cursor && m_cursor->has_data()); }

    /**
     * Make query execute request and use response to set internal
     * state.
     *
     * @return Result.
     */
    sql_result make_request_execute();

    /**
     * Process affected keys array received from the server.
     *
     * @param affected_rows Affected keys.
     */
    void process_affected_rows(const std::vector<std::int64_t> &affected_rows);

    /**
     * Make query close request.
     *
     * @return Result.
     */
    sql_result make_request_close();

    /**
     * Make data fetch request and use response to set internal state.
     *
     * @param page Resulting page.
     * @return Result.
     */
    sql_result make_request_fetch(std::unique_ptr<result_page> &page);

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
    void set_resultset_meta(column_meta_vector value);

    /**
     * Set metadata for params.
     *
     * @param value Metadata.
     */
    void set_params_meta(std::vector<sql_parameter> value);

    /**
     * Close query.
     *
     * @return Result.
     */
    sql_result internal_close();

    /** Connection associated with the statement. */
    sql_connection &m_connection;

    /** SQL query. */
    std::string m_query;

    /** Parameter bindings. */
    const parameter_set &m_params;

    /** Parameter types. */
    std::vector<sql_parameter> m_params_meta{};

    /** Indicating if the query was executed. */
    bool m_executed{false};

    /** Parameter metadata is available. */
    volatile bool m_params_meta_available{false};

    /** Result set metadata is available */
    volatile bool m_result_meta_available{false};

    /** Result set metadata. */
    column_meta_vector m_result_meta;

    /** Query ID, nullopt when closed on server. */
    std::optional<std::int64_t> m_query_id;

    /** Indicate whether result set is available. */
    bool m_has_rowset{false};

    /** Indicate whether there are more pages available on server side. */
    bool m_has_more_pages{false};

    /** Indicate whether query was applied. */
    bool m_was_applied{false};

    /** Number of rows affected. */
    std::int64_t m_rows_affected{-1};

    /** Cursor. */
    std::unique_ptr<cursor> m_cursor;

    /** Timeout. */
    std::int32_t &m_timeout;
};

} // namespace ignite
