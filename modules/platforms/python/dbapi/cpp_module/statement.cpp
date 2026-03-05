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

#include "ignite/protocol/reader.h"

#include "node_connection.h"
#include "type_conversion.h"
#include "utils.h"
#include "statement.h"

namespace {
/**
 * Write row of the param set using a provided writer.
 *
 * @param writer Writer.
 * @param params_row Parameter Row.
 * @param row_size_expected Expected size of the row.
 */
void write_row(ignite::protocol::writer &writer, PyObject *params_row, std::int32_t row_size_expected) {
    if (!params_row || params_row == Py_None) {
        throw ignite::ignite_error(ignite::error::code::ILLEGAL_ARGUMENT, "Parameter row can not be None");
    }

    if (!PySequence_Check(params_row)) {
        throw ignite::ignite_error(ignite::error::code::ILLEGAL_ARGUMENT,
            std::string("Parameter row does not provide the sequence protocol: ")
            + py_object_get_typename(params_row));
    }

    Py_ssize_t seq_size{PySequence_Size(params_row)};
    if (seq_size < 0) {
        throw ignite::ignite_error("Internal error while getting size of the parameter list sequence");
    }

    auto row_size = std::int32_t(seq_size);
    if (row_size != row_size_expected) {
        throw ignite::ignite_error(ignite::error::code::ILLEGAL_ARGUMENT,
            "Row size is unexpected: " + std::to_string(row_size)
            + ", expected row size: " + std::to_string(row_size_expected));
    }

    ignite::binary_tuple_builder row_builder{row_size * 3};
    row_builder.start();

    for (std::int32_t idx = 0; idx < row_size; ++idx) {
        submit_pyobject(row_builder, PySequence_GetItem(params_row, idx), true);
    }

    row_builder.layout();

    for (std::int32_t idx = 0; idx < row_size; ++idx) {
        submit_pyobject(row_builder, PySequence_GetItem(params_row, idx), false);
    }

    auto row_data = row_builder.build();
    writer.write_binary(row_data);
}

/**
 * Read rows of data from the reader.
 *
 * @param reader Reader.
 * @return Rows.
 */
std::vector<ignite::bytes_view> read_rows(ignite::protocol::reader &reader) {
    auto size = reader.read_int32();

    std::vector<ignite::bytes_view> rows;
    rows.reserve(size);

    for (std::int32_t row_idx = 0; row_idx < size; ++row_idx) {
        rows.emplace_back(reader.read_binary());
    }

    return rows;
}

} // anonymous namespace

void py_parameter_set::write(ignite::protocol::writer &writer) const {
    if (!m_row_size) {
        writer.write_nil();
        return;
    }

    if (!is_batch_query()) {
        // m_params - is a sequence of parameters.
        auto size = std::int32_t(m_row_size);

        writer.write(size);
        write_row(writer, m_params, size);
    } else {
        // m_params - is a sequence of parameter sequences.
        write(writer, 0, m_size, true);
    }
}

void py_parameter_set::write(ignite::protocol::writer &writer, std::size_t begin, std::size_t end,
    bool last) const {
    Py_ssize_t interval_end = std::min(m_size, Py_ssize_t(end));
    std::int32_t rows_num = std::int32_t(interval_end) - std::int32_t(begin);

    writer.write(std::int32_t(m_row_size));
    writer.write(rows_num);
    writer.write_bool(last);

    for (auto i = Py_ssize_t(begin); i < interval_end; ++i) {
        PyObject *row = PySequence_GetItem(m_params, i);
        write_row(writer, row, std::int32_t(m_row_size));
    }
}

void statement::close() noexcept {
    if (!m_cursor)
        return;

    if (m_has_more_pages) {
        if (!m_query_id)
            return;

        auto res = m_connection->sync_request_nothrow(ignite::protocol::client_operation::SQL_CURSOR_CLOSE,
            [&](ignite::protocol::writer &writer) { writer.write(*m_query_id); });

        UNUSED_VALUE res;
    }

    m_query.clear();
    m_params_meta.clear();
    m_executed = false;
    m_cursor.reset();
    m_rows_affected = -1;
    m_params_meta_available = false;
    m_result_meta_available = false;
    m_result_meta.clear();
    m_query_id = std::nullopt;
    m_has_rowset = false;
    m_has_more_pages = false;
    m_was_applied = false;
}

void statement::execute(const char *query, py_parameter_set &params) {
    close();

    m_query = query;
    auto &schema = m_connection->get_schema();

    bool single = !params.is_batch_query();

    auto tx = m_connection->get_transaction_id();
    if (!tx && !m_connection->is_auto_commit()) {
        // Starting transaction if it's not started already.
        m_connection->transaction_start();

        tx = m_connection->get_transaction_id();
        assert(tx);
    }

    auto client_op = single
        ? ignite::protocol::client_operation::SQL_EXEC
        : ignite::protocol::client_operation::SQL_EXEC_BATCH;

    auto [resp, err] = m_connection->sync_request_nothrow(client_op, [&](ignite::protocol::writer &writer) {
        if (tx)
            writer.write(*tx);
        else
            writer.write_nil();

        writer.write(schema);
        writer.write(m_connection->get_page_size());
        writer.write(std::int64_t(m_connection->get_timeout()) * 1000);
        writer.write_nil(); // Session timeout (unused, session is closed by the server immediately).
        writer.write_nil(); // Timezone

        // Properties are not used for now.
        writer.write(0);
        ignite::binary_tuple_builder prop_builder{0};
        prop_builder.start();
        prop_builder.layout();
        auto prop_data = prop_builder.build();
        writer.write_binary(prop_data);

        writer.write(m_query);
        params.write(writer);
        writer.write(m_connection->get_observable_timestamp());
    });

    // Check error
    if (err) {
        if (!single) {
            auto affected_rows = err->get_extra<std::vector<std::int64_t>>(
                ignite::protocol::error_extensions::SQL_UPDATE_COUNTERS);
            if (affected_rows) {
                process_affected_rows(*affected_rows, params);
            }
        }

        throw std::move(*err);
    }

    m_connection->mark_transaction_non_empty();

    auto &response = resp;
    ignite::protocol::reader reader(response.get_bytes_view());
    m_query_id = reader.read_object_nullable<std::int64_t>();

    m_has_rowset = reader.read_bool();
    m_has_more_pages = reader.read_bool();
    m_was_applied = reader.read_bool();

    if (single) {
        m_rows_affected = reader.read_int64();

        if (m_has_rowset) {
            auto columns = read_result_set_meta(reader);
            set_resultset_meta(std::move(columns));
            auto rows = read_rows(reader);

            auto page = std::make_unique<result_page>(std::move(response), std::move(rows));
            m_cursor = std::make_unique<cursor>(std::move(page));
        }

        m_executed = true;
    } else {
        auto affected_rows = reader.read_int64_array();
        process_affected_rows(affected_rows, params);
    }
}

void statement::process_affected_rows(const std::vector<std::int64_t> &affected_rows, py_parameter_set &params) {
    m_rows_affected = 0;
    for (auto &ar : affected_rows) {
        m_rows_affected += ar;
    }

    m_executed = true;
}

void statement::set_resultset_meta(ignite::protocol::column_meta_vector value) {
    m_result_meta = std::move(value);
    m_result_meta_available = true;
}

void statement::set_params_meta(std::vector<sql_parameter> value) {
    m_params_meta = std::move(value);
    m_params_meta_available = true;
}

bool statement::fetch_next_row() {
    if (!m_executed) {
        throw ignite::ignite_error(
            ignite::error::code::CURSOR_ALREADY_CLOSED, "Query was not executed or cursor was already closed.");
    }

    if (!m_has_rowset || !m_cursor)
        return false;

    m_cursor->next(m_result_meta);

    if (!is_data_available())
        return false;

    if (!m_cursor->has_data()) {
        if (!m_query_id) {
            throw ignite::ignite_error(
                ignite::error::code::CURSOR_ALREADY_CLOSED, "Cursor already closed.");
        }

        auto [response, err] = m_connection->sync_request_nothrow(
            ignite::protocol::client_operation::SQL_CURSOR_NEXT_PAGE,
            [&](ignite::protocol::writer &writer) { writer.write(*m_query_id); });

        if (err) {
            throw std::move(*err);
        }

        ignite::protocol::reader reader(response.get_bytes_view());
        auto rows = read_rows(reader);
        m_has_more_pages = reader.read_bool();

        auto page = std::make_unique<result_page>(std::move(response), std::move(rows));

        m_cursor->update_data(std::move(page));
        m_cursor->next(m_result_meta);
    }

    return m_cursor->has_data();
}

void statement::update_meta() {
    auto &schema = m_connection->get_schema();

    auto tx = m_connection->get_transaction_id();
    auto [response, err] = m_connection->sync_request_nothrow(ignite::protocol::client_operation::SQL_QUERY_META,
        [&](ignite::protocol::writer &writer) {
            if (tx)
                writer.write(*tx);
            else
                writer.write_nil();

            writer.write(schema);
            writer.write(m_query);
        });

    if (tx) {
        m_connection->mark_transaction_non_empty();
    }

    if (err) {
        throw std::move(*err);
    }

    auto reader = std::make_unique<ignite::protocol::reader>(response.get_bytes_view());
    auto num = reader->read_int32();
    if (num < 0) {
        throw ignite::ignite_error(
            ignite::error::code::PROTOCOL, "Unexpected number of parameters: " + std::to_string(num));
    }

    std::vector<sql_parameter> params;
    params.reserve(num);

    for (std::int32_t i = 0; i < num; ++i) {
        sql_parameter param{};
        param.nullable = reader->read_bool();
        param.data_type = ignite::ignite_type(reader->read_int32());
        param.scale = reader->read_int32();
        param.precision = reader->read_int32();

        params.emplace_back(param);
    }

    set_params_meta(std::move(params));

    auto columns = read_result_set_meta(*reader);
    set_resultset_meta(std::move(columns));
}