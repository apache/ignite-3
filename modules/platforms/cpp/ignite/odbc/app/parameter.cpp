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

#include "ignite/odbc/app/parameter.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/protocol/utils.h"

#include <algorithm>

namespace ignite {

void parameter::claim(binary_tuple_builder &builder, int offset, SQLULEN idx) const {
    if (m_buffer.get_input_size() == SQL_NULL_DATA) {
        builder.claim_null(); // Type.
        builder.claim_null(); // Scale.
        builder.claim_null(); // Value.
        return;
    }

    // Buffer to use to get data.
    application_data_buffer buf(m_buffer);
    buf.set_byte_offset(offset);
    buf.set_element_offset(idx);

    auto stored_data_len = static_cast<SQLLEN>(m_stored_data.size());

    if (m_buffer.is_data_at_exec()) {
        buf = application_data_buffer(
            m_buffer.get_type(), const_cast<std::byte *>(&m_stored_data[0]), stored_data_len, &stored_data_len);
    }

    switch (m_sql_type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR: {
            protocol::claim_type(builder, ignite_type::STRING);
            builder.claim_varlen(buf.get_string(m_column_size));
            break;
        }

        case SQL_TINYINT: {
            protocol::claim_type(builder, ignite_type::INT8);
            builder.claim_int8(buf.get_int8());
            break;
        }

        case SQL_SMALLINT: {
            protocol::claim_type(builder, ignite_type::INT16);
            builder.claim_int16(buf.get_int16());
            break;
        }

        case SQL_INTEGER: {
            protocol::claim_type(builder, ignite_type::INT32);
            builder.claim_int32(buf.get_int32());
            break;
        }

        case SQL_BIGINT: {
            protocol::claim_type(builder, ignite_type::INT64);
            builder.claim_int64(buf.get_int64());
            break;
        }

        case SQL_FLOAT: {
            protocol::claim_type(builder, ignite_type::FLOAT);
            builder.claim_float(buf.get_float());
            break;
        }

        case SQL_DOUBLE: {
            protocol::claim_type(builder, ignite_type::DOUBLE);
            builder.claim_double(buf.get_double());
            break;
        }

        case SQL_BIT: {
            protocol::claim_type(builder, ignite_type::BOOLEAN);
            builder.claim_bool(buf.get_int8() != 0);
            break;
        }

        case SQL_TYPE_DATE:
        case SQL_DATE: {
            protocol::claim_type(builder, ignite_type::DATE);
            builder.claim_date(buf.get_date());
            break;
        }

        case SQL_TYPE_TIMESTAMP:
        case SQL_TIMESTAMP: {
            protocol::claim_type(builder, ignite_type::DATETIME);
            builder.claim_date_time(buf.get_date_time());
            break;
        }

        case SQL_TYPE_TIME:
        case SQL_TIME: {
            protocol::claim_type(builder, ignite_type::TIME);
            builder.claim_time(buf.get_time());
            break;
        }

        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY: {
            protocol::claim_type(builder, ignite_type::BYTE_ARRAY);

            const application_data_buffer &const_buf = buf;
            const SQLLEN *res_len_ptr = const_buf.get_result_len();
            if (!res_len_ptr)
                break;

            auto param_len = static_cast<std::size_t>(*res_len_ptr);
            builder.claim_varlen({const_buf.get_data(), param_len});
            break;
        }

        case SQL_GUID: {
            protocol::claim_type(builder, ignite_type::UUID);
            builder.claim_uuid(buf.get_uuid());

            break;
        }

        case SQL_DECIMAL: {
            big_decimal dec_value;
            buf.get_decimal(dec_value);

            protocol::claim_type(builder, ignite_type::DECIMAL);
            builder.claim_number(dec_value);
            break;
        }

        default:
            break;
    }
}

void parameter::append(binary_tuple_builder &builder, int offset, SQLULEN idx) const {
    if (m_buffer.get_input_size() == SQL_NULL_DATA) {
        builder.append_null(); // Type.
        builder.append_null(); // Scale.
        builder.append_null(); // Value.
        return;
    }

    // Buffer to use to get data.
    application_data_buffer buf(m_buffer);
    buf.set_byte_offset(offset);
    buf.set_element_offset(idx);

    auto stored_data_len = static_cast<SQLLEN>(m_stored_data.size());

    if (m_buffer.is_data_at_exec()) {
        buf = application_data_buffer(
            m_buffer.get_type(), const_cast<std::byte *>(&m_stored_data[0]), stored_data_len, &stored_data_len);
    }

    switch (m_sql_type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR: {
            protocol::append_type(builder, ignite_type::STRING);
            builder.append_varlen(buf.get_string(m_column_size));
            break;
        }

        case SQL_TINYINT: {
            protocol::append_type(builder, ignite_type::INT8);
            builder.append_int8(buf.get_int8());
            break;
        }

        case SQL_SMALLINT: {
            protocol::append_type(builder, ignite_type::INT16);
            builder.append_int16(buf.get_int16());
            break;
        }

        case SQL_INTEGER: {
            protocol::append_type(builder, ignite_type::INT32);
            builder.append_int32(buf.get_int32());
            break;
        }

        case SQL_BIGINT: {
            protocol::append_type(builder, ignite_type::INT64);
            builder.append_int64(buf.get_int64());
            break;
        }

        case SQL_FLOAT: {
            protocol::append_type(builder, ignite_type::FLOAT);
            builder.append_float(buf.get_float());
            break;
        }

        case SQL_DOUBLE: {
            protocol::append_type(builder, ignite_type::DOUBLE);
            builder.append_double(buf.get_double());
            break;
        }

        case SQL_BIT: {
            protocol::append_type(builder, ignite_type::BOOLEAN);
            builder.append_bool(buf.get_int8() != 0);
            break;
        }

        case SQL_TYPE_DATE:
        case SQL_DATE: {
            protocol::append_type(builder, ignite_type::DATE);
            builder.append_date(buf.get_date());
            break;
        }

        case SQL_TYPE_TIMESTAMP:
        case SQL_TIMESTAMP: {
            protocol::append_type(builder, ignite_type::DATETIME);
            builder.append_date_time(buf.get_date_time());
            break;
        }

        case SQL_TYPE_TIME:
        case SQL_TIME: {
            protocol::append_type(builder, ignite_type::TIME);
            builder.append_time(buf.get_time());
            break;
        }

        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY: {
            protocol::append_type(builder, ignite_type::BYTE_ARRAY);

            const application_data_buffer &const_buf = buf;
            const SQLLEN *res_len_ptr = const_buf.get_result_len();
            if (!res_len_ptr)
                break;

            auto param_len = static_cast<std::size_t>(*res_len_ptr);
            builder.append_varlen({const_buf.get_data(), param_len});
            break;
        }

        case SQL_GUID: {
            protocol::append_type(builder, ignite_type::UUID);
            builder.append_uuid(buf.get_uuid());

            break;
        }

        case SQL_DECIMAL: {
            big_decimal dec_value;
            buf.get_decimal(dec_value);

            protocol::append_type(builder, ignite_type::DECIMAL);
            builder.append_number(dec_value);
            break;
        }

        default:
            break;
    }
}

application_data_buffer &parameter::get_buffer() {
    return m_buffer;
}

const application_data_buffer &parameter::get_buffer() const {
    return m_buffer;
}

void parameter::reset_stored_data() {
    m_stored_data.clear();

    if (m_buffer.is_data_at_exec())
        m_stored_data.reserve(m_buffer.get_data_at_exec_size());
}

bool parameter::is_data_ready() const {
    return !m_buffer.is_data_at_exec() || static_cast<SQLLEN>(m_stored_data.size()) == m_buffer.get_data_at_exec_size();
}

void parameter::put_data(void *data, SQLLEN len) {
    if (len == SQL_DEFAULT_PARAM)
        return;

    if (len == SQL_NULL_DATA) {
        m_null_data = true;
        return;
    }

    if (m_buffer.get_type() == odbc_native_type::AI_CHAR || m_buffer.get_type() == odbc_native_type::AI_BINARY) {
        SQLLEN s_len = len;

        if (m_buffer.get_type() == odbc_native_type::AI_CHAR && s_len == SQL_NTSL) {
            const char *str = reinterpret_cast<char *>(data);

            s_len = SQLLEN(std::strlen(str));
        }

        if (s_len <= 0)
            return;

        size_t begin_pos = m_stored_data.size();
        m_stored_data.resize(m_stored_data.size() + static_cast<size_t>(s_len));
        memcpy(&m_stored_data[begin_pos], data, static_cast<size_t>(s_len));

        return;
    }

    size_t data_size = m_buffer.get_data_at_exec_size();
    m_stored_data.resize(data_size);
    memcpy(&m_stored_data[0], data, data_size);
}

} // namespace ignite
