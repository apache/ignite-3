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

#include "cursor.h"

#include "ignite/protocol/writer.h"
#include "ignite/protocol/sql/column_meta.h"

#include <Python.h>

class node_connection;

/**
 * SQL parameter.
 */
struct sql_parameter {
    bool nullable;
    ignite::ignite_type data_type;
    std::int32_t scale;
    std::int32_t precision;
};

/**
 * Python parameter list set.
 */
class py_parameter_set final {
public:
    /**
     * Constructor for the case of a single parameter row.
     *
     * @param row_size Number of params in a single row.
     * @param params Python parameter sequence list.
     */
    py_parameter_set(Py_ssize_t row_size, PyObject *params)
        : m_size(0)
        , m_row_size(row_size)
        , m_params(params) {}

    /**
     * Constructor for the case of multiple parameter rows.
     *
     * @param size Size number of rows to insert.
     * @param row_size Number of params in a single row.
     * @param params Python parameter sequence list.
     */
    py_parameter_set(Py_ssize_t size, Py_ssize_t row_size, PyObject *params)
        : m_size(size)
        , m_row_size(row_size)
        , m_params(params) {}

    /**
     * Write only the first row of the param set using a provided writer.
     *
     * @param writer Writer.
     */
    void write(ignite::protocol::writer &writer) const;

    /**
     * Check if is the batch query.
     *
     * @return @c true if is the batch query.
     */
    [[nodiscard]] bool is_batch_query() const {
        return m_size > 0;
    }

private:
    /**
     * Write rows of the param set in the interval [begin, end) using the provided writer.
     *
     * @param writer Writer.
     * @param begin Beginning of the interval.
     * @param end End of the interval.
     * @param last Last page flag.
     */
    void write(ignite::protocol::writer &writer, std::size_t begin, std::size_t end, bool last) const;

    /** Rows number. */
    Py_ssize_t m_size{0};

    /** Row size. */
    Py_ssize_t m_row_size{0};

    /** Python sequence of parameters. */
    PyObject *m_params{nullptr};
};

/**
 * A result set cursor.
 */
class statement final {
public:
    /**
     * Constructor.
     *
     * @param connection Connection.
     */
    explicit statement(node_connection &connection)
        : m_connection(connection) { }

    /**
     * Destructor.
     */
    ~statement() {
        close();
    }

    /**
     * Close the statement.
     */
    void close() noexcept;

    /**
     * Execute the statement.
     * @param query SQL Query.
     * @param params Parameters.
     */
    void execute(const char *query, py_parameter_set &params);

    /**
     * Get the number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    [[nodiscard]] std::int64_t get_affected_rows() const { return m_rows_affected; }

    /**
     * Check if data is available.
     *
     * @return True if data is available.
     */
    [[nodiscard]] bool is_data_available() const { return m_has_more_pages || (m_cursor && m_cursor->has_data()); }

    /**
     * Check if the query is executed.
     *
     * @return @c true if executed and @c false, otherwise.
     */
    [[nodiscard]] bool is_executed() const { return m_executed; }

    /**
     * Get the current row.
     *
     * @return  Row.
     */
    [[nodiscard]] const std::vector<ignite::primitive> &get_current_row() const { return m_cursor->get_row(); }

    /**
     * Get column metadata.
     *
     * @return Column metadata.
     */
    [[nodiscard]] const ignite::protocol::column_meta_vector *get_meta() {
        if (!m_result_meta_available) {
            update_meta();

            if (!m_result_meta_available)
                return nullptr;
        }

        return &m_result_meta;
    }

    /**
     * Fetch next result row to application buffers.
     *
     * @return @c true if there was another row and @c false if there is no more data.
     */
    bool fetch_next_row();

private:
    /**
     * Process affected keys array received from the server.
     *
     * @param affected_rows Affected keys.
     * @param params Parameters.
     */
    void process_affected_rows(const std::vector<std::int64_t> &affected_rows, py_parameter_set &params);

    /**
     * Set metadata for the result set.
     *
     * @param value Metadata.
     */
    void set_resultset_meta(ignite::protocol::column_meta_vector value);

    /**
     * Make result set metadata request.
     *
     * @return Result.
     */
    void update_meta();

    /**
     * Set metadata for params.
     *
     * @param value Metadata.
     */
    void set_params_meta(std::vector<sql_parameter> value);

    /** Connection associated with the statement. */
    node_connection &m_connection;

    /** SQL query. */
    std::string m_query;

    /** Parameter types. */
    std::vector<sql_parameter> m_params_meta{};

    /** Indicating if the query was executed. */
    bool m_executed{false};

    /** Parameter metadata is available. */
    volatile bool m_params_meta_available{false};

    /** Result set metadata is available */
    volatile bool m_result_meta_available{false};

    /** A result set metadata. */
    ignite::protocol::column_meta_vector m_result_meta;

    /** Query ID, nullopt when closed on server. */
    std::optional<std::int64_t> m_query_id;

    /** Indicate whether a result set is available. */
    bool m_has_rowset{false};

    /** Indicate whether there are more pages available on the server side. */
    bool m_has_more_pages{false};

    /** Indicate whether the query was applied. */
    bool m_was_applied{false};

    /** Number of rows affected. */
    std::int64_t m_rows_affected{-1};

    /** Cursor. */
    std::unique_ptr<cursor> m_cursor;
};
