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

#include <set>
#include <string>

#include "diagnostic_record_storage.h"

namespace ignite {

void diagnostic_record_storage::set_header_record(sql_result result) {
    m_row_count = 0;
    m_dynamic_function.clear();
    m_dynamic_function_code = 0;
    m_result = result;
    m_rows_affected = 0;
}

void diagnostic_record_storage::add_status_record(sql_state sql_state, const std::string &message) {
    m_status_records.emplace_back(sql_state, message, "", "", 0, 0);
}

void diagnostic_record_storage::add_status_record(const diagnostic_record &record) {
    m_status_records.push_back(record);
}

void diagnostic_record_storage::reset() {
    set_header_record(sql_result::AI_ERROR);

    m_status_records.clear();
}

sql_result diagnostic_record_storage::get_operation_result() const {
    return m_result;
}

SQLRETURN diagnostic_record_storage::get_return_code() const {
    return sql_result_to_return_code(m_result);
}

int64_t diagnostic_record_storage::get_row_count() const {
    return m_row_count;
}

const std::string &diagnostic_record_storage::get_dynamic_function() const {
    return m_dynamic_function;
}

int32_t diagnostic_record_storage::get_dynamic_function_code() const {
    return m_dynamic_function_code;
}

int32_t diagnostic_record_storage::get_rows_affected() const {
    return m_rows_affected;
}

int32_t diagnostic_record_storage::get_status_records_number() const {
    return static_cast<int32_t>(m_status_records.size());
}

const diagnostic_record &diagnostic_record_storage::get_status_record(int32_t idx) const {
    return m_status_records[idx - 1];
}

diagnostic_record &diagnostic_record_storage::get_status_record(int32_t idx) {
    return m_status_records[idx - 1];
}

int32_t diagnostic_record_storage::get_last_non_retrieved() const {
    for (size_t i = 0; i < m_status_records.size(); ++i) {
        const diagnostic_record &record = m_status_records[i];

        if (!record.is_retrieved())
            return static_cast<int32_t>(i + 1);
    }

    return 0;
}

bool diagnostic_record_storage::is_successful() const {
    return m_result == sql_result::AI_SUCCESS || m_result == sql_result::AI_SUCCESS_WITH_INFO;
}

sql_result diagnostic_record_storage::get_field(
    int32_t rec_num, diagnostic_field field, application_data_buffer &buffer) const {
    // Header record.
    switch (field) {
        case diagnostic_field::HEADER_CURSOR_ROW_COUNT: {
            buffer.put_int64(get_row_count());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::HEADER_DYNAMIC_FUNCTION: {
            buffer.put_string(get_dynamic_function());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::HEADER_DYNAMIC_FUNCTION_CODE: {
            buffer.put_int32(get_dynamic_function_code());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::HEADER_NUMBER: {
            buffer.put_int32(get_status_records_number());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::HEADER_RETURN_CODE: {
            buffer.put_int32(get_return_code());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::HEADER_ROW_COUNT: {
            buffer.put_int64(get_rows_affected());

            return sql_result::AI_SUCCESS;
        }

        default:
            break;
    }

    if (rec_num < 1 || static_cast<size_t>(rec_num) > m_status_records.size())
        return sql_result::AI_NO_DATA;

    // Status record.
    const diagnostic_record &record = get_status_record(rec_num);

    switch (field) {
        case diagnostic_field::STATUS_CLASS_ORIGIN: {
            buffer.put_string(record.get_class_origin());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_COLUMN_NUMBER: {
            buffer.put_int32(record.get_column_number());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_CONNECTION_NAME: {
            buffer.put_string(record.get_connection_name());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_MESSAGE_TEXT: {
            buffer.put_string(record.get_message_text());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_NATIVE: {
            buffer.put_int32(0);

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_ROW_NUMBER: {
            buffer.put_int64(record.get_row_number());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_SERVER_NAME: {
            buffer.put_string(record.get_server_name());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_SQLSTATE: {
            buffer.put_string(record.get_sql_state());

            return sql_result::AI_SUCCESS;
        }

        case diagnostic_field::STATUS_SUBCLASS_ORIGIN: {
            buffer.put_string(record.get_subclass_origin());

            return sql_result::AI_SUCCESS;
        }

        default:
            break;
    }

    return sql_result::AI_ERROR;
}

} // namespace ignite
