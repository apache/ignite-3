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

#include <algorithm>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/app/parameter.h"

namespace ignite {

void parameter::write(protocol::writer& writer, int offset, SQLULEN idx) const
{
    if (m_buffer.get_input_size() == SQL_NULL_DATA)
    {
        writer.write_nil();
        return;
    }

    // Buffer to use to get data.
    application_data_buffer buf(m_buffer);
    buf.set_byte_offset(offset);
    buf.set_element_offset(idx);

    auto stored_data_len = static_cast<SQLLEN>(m_stored_data.size());

    if (m_buffer.is_data_at_exec())
    {
        buf = application_data_buffer(m_buffer.get_type(),
            const_cast<std::int8_t*>(&m_stored_data[0]), stored_data_len, &stored_data_len);
    }

    switch (m_sql_type)
    {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        {
            utility::WriteString(writer, buf.get_string(m_column_size));
            break;
        }

        case SQL_TINYINT:
        {
            writer.write(buf.get_int8());
            break;
        }

        case SQL_SMALLINT:
        {
            writer.write(buf.get_int16());
            break;
        }

        case SQL_INTEGER:
        {
            writer.write(buf.get_int32());
            break;
        }

        case SQL_BIGINT:
        {
            writer.write(buf.get_int64());
            break;
        }

        case SQL_FLOAT:
        {
            // TODO: IGNITE-19205 Implement proper parameter data writing.
            //writer.write(buf.get_float());
            break;
        }

        case SQL_DOUBLE:
        {
            // TODO: IGNITE-19205 Implement proper parameter data writing.
            //writer.write<double>(buf.get_double());
            break;
        }

        case SQL_BIT:
        {
            writer.write_bool(buf.get_int8() != 0);
            break;
        }

        case SQL_TYPE_DATE:
        case SQL_DATE:
        {
            // TODO: IGNITE-19205 Implement proper parameter data writing.
            //writer.write(buf.get_date());
            break;
        }

        case SQL_TYPE_TIMESTAMP:
        case SQL_TIMESTAMP:
        {
            // TODO: IGNITE-19205 Implement proper parameter data writing.
            //writer.write(buf.get_timestamp());
            break;
        }

        case SQL_TYPE_TIME:
        case SQL_TIME:
        {
            // TODO: IGNITE-19205 Implement proper parameter data writing.
            //writer.write(buf.get_time());
            break;
        }

        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
        {
            const application_data_buffer& const_buf = buf;
            const SQLLEN* res_len_ptr = const_buf.get_result_len();
            if (!res_len_ptr)
                break;

            auto param_len = static_cast<std::size_t>(*res_len_ptr);
            writer.write_binary({const_buf.get_data(), param_len});

            break;
        }

        case SQL_GUID:
        {
            writer.write(buf.get_uuid());

            break;
        }

        case SQL_DECIMAL:
        {
            big_decimal dec;
            buf.get_decimal(dec);

            utility::WriteDecimal(writer, dec);

            break;
        }

        default:
            break;
    }
}

application_data_buffer& parameter::get_buffer()
{
    return m_buffer;
}

const application_data_buffer& parameter::get_buffer() const
{
    return m_buffer;
}

void parameter::reset_stored_data()
{
    m_stored_data.clear();

    if (m_buffer.is_data_at_exec())
        m_stored_data.reserve(m_buffer.get_data_at_exec_size());
}

bool parameter::is_data_ready() const
{
    return !m_buffer.is_data_at_exec() ||
        static_cast<SQLLEN>(m_stored_data.size()) == m_buffer.get_data_at_exec_size();
}

void parameter::put_data(void* data, SQLLEN len)
{
    if (len == SQL_DEFAULT_PARAM)
        return;

    if (len == SQL_NULL_DATA)
    {
        m_null_data = true;
        return;
    }

    if (m_buffer.get_type() == odbc_native_type::AI_CHAR ||
        m_buffer.get_type() == odbc_native_type::AI_BINARY)
    {
        SQLLEN s_len = len;

        if (m_buffer.get_type() == odbc_native_type::AI_CHAR && s_len == SQL_NTSL)
        {
            const char* str = reinterpret_cast<char*>(data);

            s_len = SQLLEN(std::strlen(str));
        }

        if (s_len <= 0)
            return;

        size_t beginPos = m_stored_data.size();
        m_stored_data.resize(m_stored_data.size() + static_cast<size_t>(s_len));
        memcpy(&m_stored_data[beginPos], data, static_cast<size_t>(s_len));

        return;
    }

    size_t dataSize = m_buffer.get_data_at_exec_size();
    m_stored_data.resize(dataSize);
    memcpy(&m_stored_data[0], data, dataSize);
}

} // namespace ignite
