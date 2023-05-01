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

#include "../utility.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "parameter.h"

namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            Parameter::Parameter() :
                buffer(),
                sqlType(),
                columnSize(),
                decDigits(),
                nullData(false),
                storedData()
            {
                // No-op.
            }

            Parameter::Parameter(const application_data_buffer& buffer, int16_t sqlType,
                size_t columnSize, int16_t decDigits) :
                buffer(buffer),
                sqlType(sqlType),
                columnSize(columnSize),
                decDigits(decDigits),
                nullData(false),
                storedData()
            {
                // No-op.
            }

            Parameter::Parameter(const Parameter& other) :
                buffer(other.buffer),
                sqlType(other.sqlType),
                columnSize(other.columnSize),
                decDigits(other.decDigits),
                nullData(other.nullData),
                storedData(other.storedData)
            {
                // No-op.
            }

            Parameter::~Parameter()
            {
                // No-op.
            }

            Parameter& Parameter::operator=(const Parameter &other)
            {
                buffer = other.buffer;
                sqlType = other.sqlType;
                columnSize = other.columnSize;
                decDigits = other.decDigits;

                return *this;
            }

            void Parameter::Write(impl::binary::BinaryWriterImpl& writer, int offset, SQLULEN idx) const
            {
                if (buffer.get_input_size() == SQL_NULL_DATA)
                {
                    writer.WriteNull();

                    return;
                }

                // Buffer to use to get data.
                application_data_buffer buf(buffer);
                buf.set_byte_offset(offset);
                buf.set_element_offset(idx);

                SQLLEN storedDataLen = static_cast<SQLLEN>(storedData.size());

                if (buffer.is_data_at_exec())
                {
                    buf = application_data_buffer(buffer.get_type(),
                        const_cast<int8_t*>(&storedData[0]), storedDataLen, &storedDataLen);
                }

                switch (sqlType)
                {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                    {
                        utility::WriteString(writer, buf.get_string(columnSize));
                        break;
                    }

                    case SQL_SMALLINT:
                    {
                        writer.WriteObject<int16_t>(buf.get_int16());
                        break;
                    }

                    case SQL_INTEGER:
                    {
                        writer.WriteObject<int32_t>(buf.get_int32());
                        break;
                    }

                    case SQL_FLOAT:
                    {
                        writer.WriteObject<float>(buf.get_float());
                        break;
                    }

                    case SQL_DOUBLE:
                    {
                        writer.WriteObject<double>(buf.get_double());
                        break;
                    }

                    case SQL_TINYINT:
                    {
                        writer.WriteObject<int8_t>(buf.get_int8());
                        break;
                    }

                    case SQL_BIT:
                    {
                        writer.WriteObject<bool>(buf.get_int8() != 0);
                        break;
                    }

                    case SQL_BIGINT:
                    {
                        writer.WriteObject<int64_t>(buf.get_int64());
                        break;
                    }

                    case SQL_TYPE_DATE:
                    case SQL_DATE:
                    {
                        writer.WriteDate(buf.get_date());
                        break;
                    }

                    case SQL_TYPE_TIMESTAMP:
                    case SQL_TIMESTAMP:
                    {
                        writer.WriteTimestamp(buf.get_timestamp());
                        break;
                    }

                    case SQL_TYPE_TIME:
                    case SQL_TIME:
                    {
                        writer.WriteTime(buf.get_time());
                        break;
                    }

                    case SQL_BINARY:
                    case SQL_VARBINARY:
                    case SQL_LONGVARBINARY:
                    {
                        const application_data_buffer& constRef = buf;

                        const SQLLEN* resLenPtr = constRef.get_result_len();

                        if (!resLenPtr)
                            break;

                        int32_t paramLen = static_cast<int32_t>(*resLenPtr);

                        writer.WriteInt8Array(reinterpret_cast<const int8_t*>(constRef.get_data()), paramLen);

                        break;
                    }

                    case SQL_GUID:
                    {
                        writer.WriteGuid(buf.get_uuid());

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

            application_data_buffer& Parameter::GetBuffer()
            {
                return buffer;
            }

            const application_data_buffer& Parameter::GetBuffer() const
            {
                return buffer;
            }

            void Parameter::ResetStoredData()
            {
                storedData.clear();

                if (buffer.is_data_at_exec())
                    storedData.reserve(buffer.get_data_at_exec_size());
            }

            bool Parameter::IsDataReady() const
            {
                return !buffer.is_data_at_exec() ||
                    static_cast<SQLLEN>(storedData.size()) == buffer.get_data_at_exec_size();
            }

            void Parameter::PutData(void* data, SQLLEN len)
            {
                if (len == SQL_DEFAULT_PARAM)
                    return;

                if (len == SQL_NULL_DATA)
                {
                    nullData = true;

                    return;
                }

                if (buffer.get_type() == odbc_native_type::AI_CHAR ||
                    buffer.get_type() == odbc_native_type::AI_BINARY)
                {
                    SQLLEN slen = len;

                    if (buffer.get_type() == odbc_native_type::AI_CHAR && slen == SQL_NTSL)
                    {
                        const char* str = reinterpret_cast<char*>(data);

                        slen = strlen(str);
                    }

                    if (slen <= 0)
                        return;

                    size_t beginPos = storedData.size();

                    storedData.resize(storedData.size() + static_cast<size_t>(slen));

                    memcpy(&storedData[beginPos], data, static_cast<size_t>(slen));

                    return;
                }

                size_t dataSize = buffer.get_data_at_exec_size();

                storedData.resize(dataSize);

                memcpy(&storedData[0], data, dataSize);
            }
        }
    }
}
