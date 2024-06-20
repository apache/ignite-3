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

#include <limits>

#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/column_metadata_query.h"
#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/query/foreign_keys_query.h"
#include "ignite/odbc/query/primary_keys_query.h"
#include "ignite/odbc/query/special_columns_query.h"
#include "ignite/odbc/query/table_metadata_query.h"
#include "ignite/odbc/query/type_info_query.h"
#include "ignite/odbc/sql_statement.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/utility.h"

namespace ignite {

void sql_statement::bind_column(uint16_t column_idx, int16_t target_type, void *target_value, SQLLEN buffer_length,
    SQLLEN *str_length_or_indicator) {
    IGNITE_ODBC_API_CALL(
        internal_bind_column(column_idx, target_type, target_value, buffer_length, str_length_or_indicator));
}

sql_result sql_statement::internal_bind_column(uint16_t column_idx, int16_t target_type, void *target_value,
    SQLLEN buffer_length, SQLLEN *str_length_or_indicator) {
    odbc_native_type driver_type = to_driver_type(target_type);

    if (driver_type == odbc_native_type::AI_UNSUPPORTED) {
        add_status_record(
            sql_state::SHY003_INVALID_APPLICATION_BUFFER_TYPE, "The argument TargetType was not a valid data type.");

        return sql_result::AI_ERROR;
    }

    if (buffer_length < 0) {
        add_status_record(sql_state::SHY090_INVALID_STRING_OR_BUFFER_LENGTH,
            "The value specified for the argument BufferLength was less than 0.");

        return sql_result::AI_ERROR;
    }

    if (target_value || str_length_or_indicator) {
        application_data_buffer data_buffer(driver_type, target_value, buffer_length, str_length_or_indicator);

        safe_bind_column(column_idx, data_buffer);
    } else
        safe_unbind_column(column_idx);

    return sql_result::AI_SUCCESS;
}

void sql_statement::safe_bind_column(uint16_t column_idx, const application_data_buffer &buffer) {
    m_column_bindings[column_idx] = buffer;
}

void sql_statement::safe_unbind_column(uint16_t column_idx) {
    m_column_bindings.erase(column_idx);
}

void sql_statement::safe_unbind_all_columns() {
    m_column_bindings.clear();
}

void sql_statement::set_column_bind_offset_ptr(int *ptr) {
    m_column_bind_offset = ptr;
}

int *sql_statement::get_column_bind_offset_ptr() {
    return m_column_bind_offset;
}

int32_t sql_statement::get_column_number() {
    int32_t res;

    IGNITE_ODBC_API_CALL(internal_get_column_number(res));

    return res;
}

sql_result sql_statement::internal_get_column_number(int32_t &res) {
    const column_meta_vector *meta = get_meta();

    if (!meta)
        return sql_result::AI_ERROR;

    res = static_cast<int32_t>(meta->size());

    return sql_result::AI_SUCCESS;
}

void sql_statement::bind_parameter(uint16_t param_idx, int16_t io_type, int16_t buffer_type, int16_t param_sql_type,
    SQLULEN column_size, int16_t dec_digits, void *buffer, SQLLEN buffer_len, SQLLEN *res_len) {
    IGNITE_ODBC_API_CALL(internal_bind_parameter(
        param_idx, io_type, buffer_type, param_sql_type, column_size, dec_digits, buffer, buffer_len, res_len));
}

sql_result sql_statement::internal_bind_parameter(uint16_t param_idx, int16_t io_type, int16_t buffer_type,
    int16_t param_sql_type, SQLULEN column_size, int16_t dec_digits, void *buffer, SQLLEN buffer_len, SQLLEN *res_len) {
    if (param_idx == 0) {
        std::stringstream builder;
        builder << "The value specified for the argument ParameterNumber was less than 1. [ParameterNumber="
                << param_idx << ']';

        add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, builder.str());

        return sql_result::AI_ERROR;
    }

    if (io_type != SQL_PARAM_INPUT) {
        std::stringstream builder;
        builder << "The value specified for the argument InputOutputType was not SQL_PARAM_INPUT. [io_type=" << io_type
                << ']';

        add_status_record(sql_state::SHY105_INVALID_PARAMETER_TYPE, builder.str());

        return sql_result::AI_ERROR;
    }

    if (!is_sql_type_supported(param_sql_type)) {
        std::stringstream builder;
        builder << "Data type is not supported. [typeId=" << param_sql_type << ']';

        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, builder.str());

        return sql_result::AI_ERROR;
    }

    odbc_native_type driver_type = to_driver_type(buffer_type);

    if (driver_type == odbc_native_type::AI_UNSUPPORTED) {
        std::stringstream builder;
        builder << "The argument TargetType was not a valid data type. [TargetType=" << buffer_type << ']';

        add_status_record(sql_state::SHY003_INVALID_APPLICATION_BUFFER_TYPE, builder.str());

        return sql_result::AI_ERROR;
    }

    if (!buffer && !res_len) {
        add_status_record(sql_state::SHY009_INVALID_USE_OF_NULL_POINTER,
            "ParameterValuePtr and StrLen_or_IndPtr are both null pointers");

        return sql_result::AI_ERROR;
    }

    application_data_buffer data_buffer(driver_type, buffer, buffer_len, res_len);

    parameter param(data_buffer, param_sql_type, column_size, dec_digits);

    m_parameters.bind_parameter(param_idx, param);

    return sql_result::AI_SUCCESS;
}

void sql_statement::set_attribute(int attr, void *value, SQLINTEGER value_len) {
    IGNITE_ODBC_API_CALL(internal_set_attribute(attr, value, value_len));
}

sql_result sql_statement::internal_set_attribute(int attr, void *value, SQLINTEGER) {
    switch (attr) {
        case SQL_ATTR_ROW_ARRAY_SIZE: {
            auto val = reinterpret_cast<SQLULEN>(value);

            LOG_MSG("SQL_ATTR_ROW_ARRAY_SIZE: " << val);

            if (val < 1) {
                add_status_record(
                    sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE, "Array size value can not be less than 1");

                return sql_result::AI_ERROR;
            }

            m_row_array_size = val;

            break;
        }

        case SQL_ATTR_ROW_BIND_TYPE: {
            auto rowBindType = reinterpret_cast<SQLULEN>(value);

            if (rowBindType != SQL_BIND_BY_COLUMN) {
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Only binding by column is currently supported");

                return sql_result::AI_ERROR;
            }

            break;
        }

        case SQL_ATTR_ROWS_FETCHED_PTR: {
            set_row_fetched_ptr(reinterpret_cast<SQLINTEGER *>(value));

            break;
        }

        case SQL_ATTR_ROW_STATUS_PTR: {
            set_row_statuses_ptr(reinterpret_cast<SQLUSMALLINT *>(value));

            break;
        }

        case SQL_ATTR_PARAM_BIND_TYPE: {
            auto paramBindType = reinterpret_cast<SQLULEN>(value);

            if (paramBindType != SQL_PARAM_BIND_BY_COLUMN) {
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Only binding by column is currently supported");

                return sql_result::AI_ERROR;
            }

            break;
        }

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
            set_param_bind_offset_ptr(reinterpret_cast<int *>(value));

            break;
        }

        case SQL_ATTR_ROW_BIND_OFFSET_PTR: {
            set_column_bind_offset_ptr(reinterpret_cast<int *>(value));

            break;
        }

        case SQL_ATTR_PARAMSET_SIZE: {
            auto size = reinterpret_cast<SQLULEN>(value);

            if (size < 1) {
                add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, "Can not set parameter set size to zero.");
                return sql_result::AI_SUCCESS_WITH_INFO;
            }

            m_parameters.set_param_set_size(size);

            break;
        }

        case SQL_ATTR_PARAMS_PROCESSED_PTR: {
            m_parameters.set_params_processed_ptr(reinterpret_cast<SQLULEN *>(value));

            break;
        }

        case SQL_ATTR_PARAM_STATUS_PTR: {
            m_parameters.set_params_status_ptr(reinterpret_cast<SQLUSMALLINT *>(value));
            break;
        }

        case SQL_ATTR_QUERY_TIMEOUT: {
            auto u_timeout = reinterpret_cast<SQLULEN>(value);

            if (u_timeout > INT32_MAX) {
                m_timeout = INT32_MAX;

                std::stringstream ss;

                ss << "Value is too big: " << u_timeout << ", changing to " << m_timeout << ".";
                std::string msg = ss.str();

                add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, msg);

                return sql_result::AI_SUCCESS_WITH_INFO;
            }

            m_timeout = static_cast<int32_t>(u_timeout);

            break;
        }

        default: {
            add_status_record(
                sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Specified attribute is not supported.");

            return sql_result::AI_ERROR;
        }
    }

    return sql_result::AI_SUCCESS;
}

void sql_statement::get_attribute(int attr, void *buf, SQLINTEGER buf_len, SQLINTEGER *value_len) {
    IGNITE_ODBC_API_CALL(internal_get_attribute(attr, buf, buf_len, value_len));
}

sql_result sql_statement::internal_get_attribute(int attr, void *buf, SQLINTEGER, SQLINTEGER *value_len) {
    if (!buf) {
        add_status_record("Data buffer is NULL.");

        return sql_result::AI_ERROR;
    }

    switch (attr) {
        case SQL_ATTR_APP_ROW_DESC:
        case SQL_ATTR_APP_PARAM_DESC:
        case SQL_ATTR_IMP_ROW_DESC:
        case SQL_ATTR_IMP_PARAM_DESC: {
            auto *val = reinterpret_cast<SQLPOINTER *>(buf);

            *val = static_cast<SQLPOINTER>(this);

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_ROW_BIND_TYPE: {
            auto *val = reinterpret_cast<SQLULEN *>(buf);

            *val = SQL_BIND_BY_COLUMN;

            break;
        }

        case SQL_ATTR_ROW_ARRAY_SIZE: {
            auto *val = reinterpret_cast<SQLINTEGER *>(buf);

            *val = static_cast<SQLINTEGER>(m_row_array_size);

            if (value_len)
                *value_len = SQL_IS_INTEGER;

            break;
        }

        case SQL_ATTR_ROWS_FETCHED_PTR: {
            auto **val = reinterpret_cast<SQLULEN **>(buf);

            *val = reinterpret_cast<SQLULEN *>(get_row_fetched_ptr());

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_ROW_STATUS_PTR: {
            auto **val = reinterpret_cast<SQLUSMALLINT **>(buf);

            *val = reinterpret_cast<SQLUSMALLINT *>(get_row_statuses_ptr());

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_PARAM_BIND_TYPE: {
            auto *val = reinterpret_cast<SQLULEN *>(buf);

            *val = SQL_PARAM_BIND_BY_COLUMN;

            break;
        }

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
            auto **val = reinterpret_cast<SQLULEN **>(buf);

            *val = reinterpret_cast<SQLULEN *>(m_parameters.get_param_bind_offset_ptr());

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_ROW_BIND_OFFSET_PTR: {
            auto **val = reinterpret_cast<SQLULEN **>(buf);

            *val = reinterpret_cast<SQLULEN *>(get_column_bind_offset_ptr());

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_PARAMSET_SIZE: {
            auto *val = reinterpret_cast<SQLULEN *>(buf);

            *val = static_cast<SQLULEN>(m_parameters.get_param_set_size());

            if (value_len)
                *value_len = SQL_IS_UINTEGER;

            break;
        }

        case SQL_ATTR_PARAMS_PROCESSED_PTR: {
            auto **val = reinterpret_cast<SQLULEN **>(buf);

            *val = m_parameters.get_params_processed_ptr();

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_PARAM_STATUS_PTR: {
            auto **val = reinterpret_cast<SQLUSMALLINT **>(buf);

            *val = m_parameters.get_params_status_ptr();

            if (value_len)
                *value_len = SQL_IS_POINTER;

            break;
        }

        case SQL_ATTR_QUERY_TIMEOUT: {
            auto *u_timeout = reinterpret_cast<SQLULEN *>(buf);

            *u_timeout = static_cast<SQLULEN>(m_timeout);

            break;
        }

        default: {
            add_status_record(
                sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Specified attribute is not supported.");

            return sql_result::AI_ERROR;
        }
    }

    return sql_result::AI_SUCCESS;
}

void sql_statement::get_parameters_number(uint16_t &param_num) {
    IGNITE_ODBC_API_CALL(internal_get_parameters_number(param_num));
}

sql_result sql_statement::internal_get_parameters_number(std::uint16_t &param_num) {
    if (!m_current_query) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

        return sql_result::AI_ERROR;
    }

    if (m_current_query->get_type() != query_type::DATA) {
        param_num = 0;

        return sql_result::AI_SUCCESS;
    }

    auto qry0 = static_cast<data_query *>(m_current_query.get());
    if (!qry0->is_param_meta_available()) {
        sql_result res = qry0->update_meta();

        if (res != sql_result::AI_SUCCESS)
            return res;
    }

    param_num = std::uint16_t(qry0->get_expected_param_num());

    return sql_result::AI_SUCCESS;
}

void sql_statement::set_param_bind_offset_ptr(int *ptr) {
    IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

    m_parameters.set_param_bind_offset_ptr(ptr);
}

void sql_statement::get_column_data(uint16_t column_idx, application_data_buffer &buffer) {
    IGNITE_ODBC_API_CALL(internal_get_column_data(column_idx, buffer));
}

sql_result sql_statement::internal_get_column_data(uint16_t column_idx, application_data_buffer &buffer) {
    if (!m_current_query) {
        add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, "Cursor is not in the open state.");

        return sql_result::AI_ERROR;
    }

    sql_result res = m_current_query->get_column(column_idx, buffer);

    return res;
}

void sql_statement::prepare_sql_query(const std::string &query) {
    IGNITE_ODBC_API_CALL(internal_prepare_sql_query(query));
}

sql_result sql_statement::internal_prepare_sql_query(const std::string &query) {
    UNUSED_VALUE(query);

    // Resetting parameter types as we are changing the query.
    m_parameters.prepare();

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<data_query>(*this, m_connection, query, m_parameters, m_timeout);

    return sql_result::AI_SUCCESS;
}

void sql_statement::execute_sql_query(const std::string &query) {
    IGNITE_ODBC_API_CALL(internal_execute_sql_query(query));
}

sql_result sql_statement::internal_execute_sql_query(const std::string &query) {
    sql_result result = internal_prepare_sql_query(query);

    if (result != sql_result::AI_SUCCESS)
        return result;

    return internal_execute_sql_query();
}

void sql_statement::execute_sql_query() {
    IGNITE_ODBC_API_CALL(internal_execute_sql_query());
}

sql_result sql_statement::internal_execute_sql_query() {
    if (!m_current_query) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");
        return sql_result::AI_ERROR;
    }

    if (m_parameters.is_data_at_exec_needed())
        return sql_result::AI_NEED_DATA;

    return m_current_query->execute();
}

void sql_statement::execute_get_columns_meta_query(
    const std::string &schema, const std::string &table, const std::string &column) {
    IGNITE_ODBC_API_CALL(internal_execute_get_columns_meta_query(schema, table, column));
}

sql_result sql_statement::internal_execute_get_columns_meta_query(
    const std::string &schema, const std::string &table, const std::string &column) {

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<column_metadata_query>(*this, m_connection, schema, table, column);
    return m_current_query->execute();
}

void sql_statement::execute_get_tables_meta_query(
    const std::string &catalog, const std::string &schema, const std::string &table, const std::string &table_type) {
    IGNITE_ODBC_API_CALL(internal_execute_get_tables_meta_query(catalog, schema, table, table_type));
}

sql_result sql_statement::internal_execute_get_tables_meta_query(
    const std::string &catalog, const std::string &schema, const std::string &table, const std::string &table_type) {

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<table_metadata_query>(*this, m_connection, catalog, schema, table, table_type);
    return m_current_query->execute();
}

void sql_statement::execute_get_foreign_keys_query(const std::string &primary_catalog,
    const std::string &primary_schema, const std::string &primary_table, const std::string &foreign_catalog,
    const std::string &foreign_schema, const std::string &foreign_table) {
    IGNITE_ODBC_API_CALL(internal_execute_get_foreign_keys_query(
        primary_catalog, primary_schema, primary_table, foreign_catalog, foreign_schema, foreign_table));
}

sql_result sql_statement::internal_execute_get_foreign_keys_query(const std::string &primary_catalog,
    const std::string &primary_schema, const std::string &primary_table, const std::string &foreign_catalog,
    const std::string &foreign_schema, const std::string &foreign_table) {

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<foreign_keys_query>(
        *this, primary_catalog, primary_schema, primary_table, foreign_catalog, foreign_schema, foreign_table);

    return m_current_query->execute();
}

void sql_statement::execute_get_primary_keys_query(
    const std::string &catalog, const std::string &schema, const std::string &table) {
    IGNITE_ODBC_API_CALL(internal_execute_get_primary_keys_query(catalog, schema, table));
}

sql_result sql_statement::internal_execute_get_primary_keys_query(
    const std::string &catalog, const std::string &schema, const std::string &table) {
    UNUSED_VALUE catalog;

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<primary_keys_query>(*this, m_connection, schema, table);

    return m_current_query->execute();
}

void sql_statement::execute_special_columns_query(uint16_t type, const std::string &catalog, const std::string &schema,
    const std::string &table, uint16_t scope, uint16_t nullable) {
    IGNITE_ODBC_API_CALL(internal_execute_special_columns_query(type, catalog, schema, table, scope, nullable));
}

sql_result sql_statement::internal_execute_special_columns_query(std::uint16_t type, const std::string &catalog,
    const std::string &schema, const std::string &table, std::uint16_t scope, std::uint16_t nullable) {

    if (type != SQL_BEST_ROWID && type != SQL_ROWVER) {
        add_status_record(sql_state::SHY097_COLUMN_TYPE_OUT_OF_RANGE, "An invalid IdentifierType value was specified.");
        return sql_result::AI_ERROR;
    }

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<special_columns_query>(*this, type, catalog, schema, table, scope, nullable);

    return m_current_query->execute();
}

void sql_statement::execute_get_type_info_query(std::int16_t sql_type) {
    IGNITE_ODBC_API_CALL(internal_execute_get_type_info_query(sql_type));
}

sql_result sql_statement::internal_execute_get_type_info_query(std::int16_t sql_type) {
    if (sql_type != SQL_ALL_TYPES && !is_sql_type_supported(sql_type)) {
        std::stringstream builder;
        builder << "Data type is not supported. [typeId=" << sql_type << ']';

        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, builder.str());

        return sql_result::AI_ERROR;
    }

    if (m_current_query)
        m_current_query->close();

    m_current_query = std::make_unique<type_info_query>(*this, sql_type);
    return m_current_query->execute();
}

void sql_statement::free_resources(uint16_t option) {
    IGNITE_ODBC_API_CALL(internal_free_resources(option));
}

sql_result sql_statement::internal_free_resources(uint16_t option) {
    switch (option) {
        case SQL_DROP: {
            add_status_record("Deprecated, call SQLFreeHandle instead");

            return sql_result::AI_ERROR;
        }

        case SQL_CLOSE: {
            return internal_close();
        }

        case SQL_UNBIND: {
            safe_unbind_all_columns();

            break;
        }

        case SQL_RESET_PARAMS: {
            m_parameters.unbind_all();
            m_parameters.set_param_set_size(0);

            break;
        }

        default: {
            add_status_record(
                sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE, "The value specified for the argument Option was invalid");
            return sql_result::AI_ERROR;
        }
    }
    return sql_result::AI_SUCCESS;
}

void sql_statement::close() {
    IGNITE_ODBC_API_CALL(internal_close());
}

sql_result sql_statement::internal_close() {
    if (!m_current_query)
        return sql_result::AI_SUCCESS;

    sql_result result = m_current_query->close();

    return result;
}

void sql_statement::fetch_scroll(int16_t orientation, int64_t offset) {
    IGNITE_ODBC_API_CALL(internal_fetch_scroll(orientation, offset));
}

sql_result sql_statement::internal_fetch_scroll(int16_t orientation, int64_t offset) {
    UNUSED_VALUE offset;

    if (orientation != SQL_FETCH_NEXT) {
        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
            "Only SQL_FETCH_NEXT FetchOrientation type is supported");

        return sql_result::AI_ERROR;
    }

    return internal_fetch_row();
}

void sql_statement::fetch_row() {
    IGNITE_ODBC_API_CALL(internal_fetch_row());
}

sql_result sql_statement::internal_fetch_row() {
    if (m_rows_fetched)
        *m_rows_fetched = 0;

    if (!m_current_query) {
        add_status_record(sql_state::S24000_INVALID_CURSOR_STATE, "Cursor is not in the open state");

        return sql_result::AI_ERROR;
    }

    if (m_column_bind_offset) {
        for (auto binding : m_column_bindings)
            binding.second.set_byte_offset(*m_column_bind_offset);
    }

    SQLINTEGER fetched = 0;
    SQLINTEGER errors = 0;

    for (SQLULEN i = 0; i < m_row_array_size; ++i) {
        for (auto binding : m_column_bindings)
            binding.second.set_element_offset(i);

        sql_result res = m_current_query->fetch_next_row(m_column_bindings);

        if (res == sql_result::AI_SUCCESS || res == sql_result::AI_SUCCESS_WITH_INFO)
            ++fetched;
        else if (res != sql_result::AI_NO_DATA)
            ++errors;

        if (m_row_statuses)
            m_row_statuses[i] = sql_result_to_row_result(res);
    }

    if (m_rows_fetched)
        *m_rows_fetched = fetched < 0 ? static_cast<SQLINTEGER>(m_row_array_size) : fetched;

    if (fetched > 0)
        return errors == 0 ? sql_result::AI_SUCCESS : sql_result::AI_SUCCESS_WITH_INFO;

    return errors == 0 ? sql_result::AI_NO_DATA : sql_result::AI_ERROR;
}

const column_meta_vector *sql_statement::get_meta() {
    if (!m_current_query) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not executed.");
        return nullptr;
    }

    return m_current_query->get_meta();
}

bool sql_statement::is_data_available() const {
    return m_current_query.get() && m_current_query->is_data_available();
}

void sql_statement::more_results() {
    IGNITE_ODBC_API_CALL(internal_more_results());
}

sql_result sql_statement::internal_more_results() {
    if (!m_current_query) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not executed.");
        return sql_result::AI_ERROR;
    }

    return m_current_query->next_result_set();
}

void sql_statement::get_column_attribute(uint16_t column_idx, uint16_t attr_id, char *string_buf, int16_t buffer_len,
    int16_t *result_len, SQLLEN *numeric_buf) {
    IGNITE_ODBC_API_CALL(
        internal_get_column_attribute(column_idx, attr_id, string_buf, buffer_len, result_len, numeric_buf));
}

sql_result sql_statement::internal_get_column_attribute(uint16_t column_idx, uint16_t attr_id, char *string_buf,
    int16_t buffer_len, int16_t *result_len, SQLLEN *numeric_buf) {
    const column_meta_vector *meta = get_meta();

    LOG_MSG("Column ID: " << column_idx << ", Attribute ID: " << attr_id);

    if (!meta)
        return sql_result::AI_ERROR;

    if (column_idx > meta->size() || column_idx < 1) {
        add_status_record(sql_state::S42S22_COLUMN_NOT_FOUND, "Column index is out of range.", 0, column_idx);

        return sql_result::AI_ERROR;
    }

    const column_meta &column_meta = meta->at(column_idx - 1);

    bool found = false;

    if (numeric_buf)
        found = column_meta.get_attribute(attr_id, *numeric_buf);

    if (!found) {
        std::string out;

        found = column_meta.get_attribute(attr_id, out);

        size_t outSize = out.size();

        if (found && string_buf)
            outSize = copy_string_to_buffer(out, string_buf, buffer_len);

        if (found && result_len)
            *result_len = static_cast<int16_t>(outSize);
    }

    if (!found) {
        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Unknown attribute.");

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

int64_t sql_statement::affected_rows() {
    int64_t row_count = 0;

    IGNITE_ODBC_API_CALL(internal_affected_rows(row_count));

    return row_count;
}

sql_result sql_statement::internal_affected_rows(int64_t &row_count) {
    if (!m_current_query) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not executed.");

        return sql_result::AI_ERROR;
    }

    row_count = m_current_query->affected_rows();

    return sql_result::AI_SUCCESS;
}

void sql_statement::set_row_fetched_ptr(SQLINTEGER *ptr) {
    m_rows_fetched = ptr;
}

SQLINTEGER *sql_statement::get_row_fetched_ptr() {
    return m_rows_fetched;
}

void sql_statement::set_row_statuses_ptr(SQLUSMALLINT *ptr) {
    m_row_statuses = ptr;
}

SQLUSMALLINT *sql_statement::get_row_statuses_ptr() {
    return m_row_statuses;
}

void sql_statement::select_param(void **param_ptr) {
    IGNITE_ODBC_API_CALL(internal_select_param(param_ptr));
}

sql_result sql_statement::internal_select_param(void **param_ptr) {
    if (!param_ptr) {
        add_status_record(sql_state::SHY000_GENERAL_ERROR, "Invalid parameter: ValuePtrPtr is null.");

        return sql_result::AI_ERROR;
    }

    if (!m_current_query) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

        return sql_result::AI_ERROR;
    }

    parameter *selected = m_parameters.get_selected_parameter();

    if (selected && !selected->is_data_ready()) {
        add_status_record(sql_state::S22026_DATA_LENGTH_MISMATCH,
            "Less data was sent for a parameter than was specified with "
            "the StrLen_or_IndPtr argument in SQLBindParameter.");

        return sql_result::AI_ERROR;
    }

    selected = m_parameters.select_next_parameter();

    if (selected) {
        *param_ptr = selected->get_buffer().get_data();

        return sql_result::AI_NEED_DATA;
    }

    sql_result res = m_current_query->execute();

    if (res != sql_result::AI_SUCCESS)
        res = sql_result::AI_SUCCESS_WITH_INFO;

    return res;
}

void sql_statement::put_data(void *data, SQLLEN len) {
    IGNITE_ODBC_API_CALL(internal_put_data(data, len));
}

sql_result sql_statement::internal_put_data(void *data, SQLLEN len) {
    if (!data && len != 0 && len != SQL_DEFAULT_PARAM && len != SQL_NULL_DATA) {
        add_status_record(sql_state::SHY009_INVALID_USE_OF_NULL_POINTER,
            "Invalid parameter: DataPtr is null StrLen_or_Ind is not 0, "
            "SQL_DEFAULT_PARAM, or SQL_NULL_DATA.");

        return sql_result::AI_ERROR;
    }

    if (!m_parameters.is_parameter_selected()) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "parameter is not selected with the SQLParamData.");

        return sql_result::AI_ERROR;
    }

    parameter *param = m_parameters.get_selected_parameter();

    if (!param) {
        add_status_record(sql_state::SHY000_GENERAL_ERROR, "Selected parameter has been unbound.");

        return sql_result::AI_ERROR;
    }

    param->put_data(data, len);

    return sql_result::AI_SUCCESS;
}

void sql_statement::describe_param(
    uint16_t param_num, int16_t *data_type, SQLULEN *param_size, int16_t *decimal_digits, int16_t *nullable) {
    IGNITE_ODBC_API_CALL(internal_describe_param(param_num, data_type, param_size, decimal_digits, nullable));
}

sql_result sql_statement::internal_describe_param(
    uint16_t param_num, int16_t *data_type, SQLULEN *param_size, int16_t *decimal_digits, int16_t *nullable) {
    query *qry = m_current_query.get();
    if (!qry) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not prepared.");
        return sql_result::AI_ERROR;
    }

    if (qry->get_type() != query_type::DATA) {
        add_status_record(sql_state::SHY010_SEQUENCE_ERROR, "Query is not SQL data query.");
        return sql_result::AI_ERROR;
    }

    auto qry0 = static_cast<data_query *>(qry);
    if (!qry0->is_param_meta_available()) {
        sql_result res = qry0->update_meta();

        if (res != sql_result::AI_SUCCESS)
            return res;
    }

    auto sql_param = qry0->get_sql_param(std::int16_t(param_num));
    if (!sql_param) {
        add_status_record(sql_state::S07009_INVALID_DESCRIPTOR_INDEX, "Parameter index is out of range.");

        return sql_result::AI_ERROR;
    }

    LOG_MSG("Type: " << (int) sql_param->data_type);

    if (data_type)
        *data_type = ignite_type_to_sql_type(sql_param->data_type);

    if (param_size)
        *param_size = sql_param->precision;

    if (decimal_digits)
        *decimal_digits = (std::int16_t) sql_param->scale;

    if (nullable)
        *nullable = sql_param->nullable ? SQL_NULLABLE : SQL_NO_NULLS;

    return sql_result::AI_SUCCESS;
}

uint16_t sql_statement::sql_result_to_row_result(sql_result value) {
    switch (value) {
        case sql_result::AI_NO_DATA:
            return SQL_ROW_NOROW;

        case sql_result::AI_SUCCESS:
            return SQL_ROW_SUCCESS;

        case sql_result::AI_SUCCESS_WITH_INFO:
            return SQL_ROW_SUCCESS_WITH_INFO;

        default:
            return SQL_ROW_ERROR;
    }
}

} // namespace ignite
