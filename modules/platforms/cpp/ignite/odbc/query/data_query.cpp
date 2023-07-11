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

#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/query/cursor.h"
#include "ignite/tuple/binary_tuple_builder.h"

#include <memory>
#include <utility>

namespace
{
using namespace ignite;

/**
 * Reads result set metadata.
 *
 * @param reader Reader.
 * @return Result set meta columns.
 */
column_meta_vector read_meta(protocol::reader &reader) {
    auto size = reader.read_array_size();

    column_meta_vector columns;
    columns.reserve(size);

    reader.read_array_raw([&columns](std::uint32_t idx, const msgpack_object &obj) {
        if (obj.type != MSGPACK_OBJECT_ARRAY)
            throw ignite_error("Meta column expected to be serialized as array");

        const msgpack_object_array &arr = obj.via.array;

        constexpr std::uint32_t min_count = 6;
        assert(arr.size >= min_count);

        auto name = protocol::unpack_object<std::string>(arr.ptr[0]);
        auto nullable = protocol::unpack_object<bool>(arr.ptr[1]);
        auto typ = ignite_type(protocol::unpack_object<std::int32_t>(arr.ptr[2]));
        auto scale = protocol::unpack_object<std::int32_t>(arr.ptr[3]);
        auto precision = protocol::unpack_object<std::int32_t>(arr.ptr[4]);

        bool origin_present = protocol::unpack_object<bool>(arr.ptr[5]);

        if (!origin_present) {
            columns.emplace_back("", "", std::move(name), typ, precision, scale, nullable);
            return;
        }

        assert(arr.size >= min_count + 3);
        auto origin_name =
            arr.ptr[6].type == MSGPACK_OBJECT_NIL ? name : protocol::unpack_object<std::string>(arr.ptr[6]);

        auto origin_schema_id = protocol::try_unpack_object<std::int32_t>(arr.ptr[7]);
        std::string origin_schema;
        if (origin_schema_id) {
            if (*origin_schema_id >= std::int32_t(columns.size())) {
                throw ignite_error("Origin schema ID is too large: " + std::to_string(*origin_schema_id)
                    + ", id=" + std::to_string(idx));
            }
            origin_schema = columns[*origin_schema_id].get_schema_name();
        } else {
            origin_schema = protocol::unpack_object<std::string>(arr.ptr[7]);
        }

        auto origin_table_id = protocol::try_unpack_object<std::int32_t>(arr.ptr[8]);
        std::string origin_table;
        if (origin_table_id) {
            if (*origin_table_id >= std::int32_t(columns.size())) {
                throw ignite_error("Origin table ID is too large: " + std::to_string(*origin_table_id)
                    + ", id=" + std::to_string(idx));
            }
            origin_table = columns[*origin_table_id].get_table_name();
        } else {
            origin_table = protocol::unpack_object<std::string>(arr.ptr[8]);
        }

        columns.emplace_back(
            std::move(origin_schema), std::move(origin_table), std::move(name), typ, precision, scale, nullable);
    });

    return columns;
}

}

namespace ignite
{

data_query::data_query(diagnosable_adapter &m_diag, sql_connection &m_connection, std::string sql,
    const parameter_set &params, std::int32_t &timeout)
    : query(m_diag, query_type::DATA)
    , m_connection(m_connection)
    , m_query(std::move(sql))
    , m_params(params)
    , m_timeout(timeout) { }

data_query::~data_query()
{
    internal_close();
}

sql_result data_query::execute()
{
    internal_close();

    return make_request_execute();
}

const column_meta_vector *data_query::get_meta()
{
    if (!m_result_meta_available) {
        make_request_resultset_meta();
        if (!m_result_meta_available)
            return nullptr;
    }

    return &m_result_meta;
}

sql_result data_query::fetch_next_row(column_binding_map &column_bindings)
{
    UNUSED_VALUE column_bindings;
    // TODO: IGNITE-19213 Implement data fetching
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data fetching is not implemented");
    return sql_result::AI_ERROR;
}

sql_result data_query::get_column(std::uint16_t column_idx, application_data_buffer &buffer)
{
    UNUSED_VALUE column_idx;
    UNUSED_VALUE buffer;
    // TODO: IGNITE-19213 Implement data fetching
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data fetching is not implemented");
    return sql_result::AI_ERROR;
}

sql_result data_query::close()
{
    return internal_close();
}

sql_result data_query::internal_close()
{
    if (!m_cursor)
        return sql_result::AI_SUCCESS;

    sql_result result = sql_result::AI_SUCCESS;

    if (!is_closed_remotely())
        result = make_request_close();

    if (result == sql_result::AI_SUCCESS)
    {
        m_cursor.reset();
        m_rows_affected = -1;
    }

    return result;
}

bool data_query::is_data_available() const
{
    return m_has_more_pages || (m_cursor && m_cursor->has_data());
}

std::int64_t data_query::affected_rows() const
{
    return m_rows_affected;
}

sql_result data_query::next_result_set()
{
    // TODO: IGNITE-19855 Multiple queries execution is not supported.
    internal_close();
    return sql_result::AI_NO_DATA;
}

bool data_query::is_closed_remotely() const
{
    return !m_has_more_pages;
}

sql_result data_query::make_request_execute()
{
    auto &schema = m_connection.get_schema();

    network::data_buffer_owning response;
    auto success = m_diag.catch_errors([&]{
        response = m_connection.sync_request(detail::client_operation::SQL_EXEC, [&](protocol::writer &writer) {
            // TODO: IGNITE-19399 Implement transactions support.
            writer.write_nil();

            writer.write(schema);
            writer.write(m_connection.get_configuration().get_page_size().get_value());
            writer.write(std::int64_t(m_connection.get_timeout()) * 1000);
            writer.write_nil(); // Session timeout (unused, session is closed by the server immediately).

            // Properties are not used for now.
            writer.write(0);
            binary_tuple_builder prop_builder{0};
            prop_builder.start();
            prop_builder.layout();
            auto prop_data = prop_builder.build();
            writer.write_binary(prop_data);

            writer.write(m_query);

            m_params.write(writer);
        });
    });

    if (!success)
        return sql_result::AI_ERROR;

    auto reader = std::make_unique<protocol::reader>(response.get_bytes_view());

    auto resource_id = reader->read_object_nullable<std::int64_t>();
    if (resource_id) {
        m_cursor = std::make_unique<cursor>(*resource_id);
    }

    m_has_rowset = reader->read_bool();
    m_has_more_pages = reader->read_bool();
    m_was_applied = reader->read_bool();
    m_rows_affected = reader->read_int64();

    if (m_has_rowset) {
        auto columns = read_meta(*reader);
        set_resultset_meta(columns);
        m_cached_page = std::make_unique<result_page>(std::move(response), std::move(reader));
    }

    return sql_result::AI_SUCCESS;
}

sql_result data_query::make_request_close()
{
    if (!m_cursor)
        return sql_result::AI_SUCCESS;

    LOG_MSG("Closing cursor: " << m_cursor->get_query_id());

    auto success = m_diag.catch_errors([&]{
        UNUSED_VALUE m_connection.sync_request(detail::client_operation::SQL_CURSOR_CLOSE,
            [&](protocol::writer &writer) {
            writer.write(m_cursor->get_query_id());
        });
    });

    return success ? sql_result::AI_SUCCESS : sql_result::AI_ERROR;
}

sql_result data_query::make_request_fetch()
{
    // TODO: IGNITE-19213 Implement data fetching
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data fetching is not implemented");
    return sql_result::AI_ERROR;
}

sql_result data_query::make_request_resultset_meta()
{
    // TODO: IGNITE-19854 Implement metadata fetching for the non-executed query.
    m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
        "Metadata for non-executed queries is not supported");

    return sql_result::AI_ERROR;
}

sql_result data_query::process_conversion_result(conversion_result conv_res, std::int32_t row_idx,
    std::int32_t column_idx)
{
    switch (conv_res)
    {
        case conversion_result::AI_SUCCESS:
        {
            return sql_result::AI_SUCCESS;
        }

        case conversion_result::AI_NO_DATA:
        {
            return sql_result::AI_NO_DATA;
        }

        case conversion_result::AI_VARLEN_DATA_TRUNCATED:
        {
            m_diag.add_status_record(sql_state::S01004_DATA_TRUNCATED,
                "Buffer is too small for the column data. Truncated from the right.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FRACTIONAL_TRUNCATED:
        {
            m_diag.add_status_record(sql_state::S01S07_FRACTIONAL_TRUNCATION,
                "Buffer is too small for the column data. Fraction truncated.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_INDICATOR_NEEDED:
        {
            m_diag.add_status_record(sql_state::S22002_INDICATOR_NEEDED,
                "Indicator is needed but not supplied for the column buffer.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_UNSUPPORTED_CONVERSION:
        {
            m_diag.add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Data conversion is not supported.", row_idx, column_idx);

            return sql_result::AI_SUCCESS_WITH_INFO;
        }

        case conversion_result::AI_FAILURE:
        default:
        {
            m_diag.add_status_record(sql_state::S01S01_ERROR_IN_ROW,
                "Can not retrieve row column.", row_idx, column_idx);

            break;
        }
    }

    return sql_result::AI_ERROR;
}

void data_query::set_resultset_meta(const column_meta_vector& value)
{
    m_result_meta.assign(value.begin(), value.end());
    m_result_meta_available = true;

    for (size_t i = 0; i < m_result_meta.size(); ++i)
    {
        column_meta& meta = m_result_meta.at(i);
        LOG_MSG("\n[" << i << "] SchemaName:     " << meta.get_schema_name()
            <<  "\n[" << i << "] TypeName:       " << meta.get_table_name()
            <<  "\n[" << i << "] ColumnName:     " << meta.get_column_name()
            <<  "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(meta.get_data_type()));
    }
}

} // namespace ignite
