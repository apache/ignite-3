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

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            DiagnosticRecordStorage::DiagnosticRecordStorage() :
                rowCount(0),
                dynamicFunction(),
                dynamicFunctionCode(0),
                result(sql_result::AI_SUCCESS),
                rowsAffected(0)
            {
                // No-op.
            }

            DiagnosticRecordStorage::~DiagnosticRecordStorage()
            {
                // No-op.
            }

            void DiagnosticRecordStorage::SetHeaderRecord(sql_result result)
            {
                rowCount = 0;
                dynamicFunction.clear();
                dynamicFunctionCode = 0;
                this->result = result;
                rowsAffected = 0;
            }

            void DiagnosticRecordStorage::AddStatusRecord(sql_state sqlState, const std::string& message)
            {
                statusRecords.push_back(DiagnosticRecord(sqlState, message, "", "", 0, 0));
            }

            void DiagnosticRecordStorage::AddStatusRecord(const DiagnosticRecord& record)
            {
                statusRecords.push_back(record);
            }

            void DiagnosticRecordStorage::Reset()
            {
                SetHeaderRecord(sql_result::AI_ERROR);

                statusRecords.clear();
            }

            sql_result DiagnosticRecordStorage::GetOperaionResult() const
            {
                return result;
            }

            int DiagnosticRecordStorage::GetReturnCode() const
            {
                return sql_result_to_return_code(result);
            }

            int64_t DiagnosticRecordStorage::GetRowCount() const
            {
                return rowCount;
            }

            const std::string & DiagnosticRecordStorage::GetDynamicFunction() const
            {
                return dynamicFunction;
            }

            int32_t DiagnosticRecordStorage::GetDynamicFunctionCode() const
            {
                return dynamicFunctionCode;
            }

            int32_t DiagnosticRecordStorage::GetRowsAffected() const
            {
                return rowsAffected;
            }

            int32_t DiagnosticRecordStorage::GetStatusRecordsNumber() const
            {
                return static_cast<int32_t>(statusRecords.size());
            }

            const DiagnosticRecord& DiagnosticRecordStorage::GetStatusRecord(int32_t idx) const
            {
                return statusRecords[idx - 1];
            }

            DiagnosticRecord& DiagnosticRecordStorage::GetStatusRecord(int32_t idx)
            {
                return statusRecords[idx - 1];
            }

            int32_t DiagnosticRecordStorage::GetLastNonRetrieved() const
            {
                for (size_t i = 0; i < statusRecords.size(); ++i)
                {
                    const DiagnosticRecord& record = statusRecords[i];

                    if (!record.IsRetrieved())
                        return static_cast<int32_t>(i + 1);
                }

                return 0;
            }

            bool DiagnosticRecordStorage::IsSuccessful() const
            {
                return result == sql_result::AI_SUCCESS ||
                       result == sql_result::AI_SUCCESS_WITH_INFO;
            }

            sql_result DiagnosticRecordStorage::GetField(int32_t recNum, diagnostic_field field, application_data_buffer& buffer) const
            {
                // Header record.
                switch (field)
                {
                    case diagnostic_field::HEADER_CURSOR_ROW_COUNT:
                    {
                        buffer.put_int64(GetRowCount());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::HEADER_DYNAMIC_FUNCTION:
                    {
                        buffer.put_string(GetDynamicFunction());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::HEADER_DYNAMIC_FUNCTION_CODE:
                    {
                        buffer.put_int32(GetDynamicFunctionCode());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::HEADER_NUMBER:
                    {
                        buffer.put_int32(GetStatusRecordsNumber());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::HEADER_RETURN_CODE:
                    {
                        buffer.put_int32(GetReturnCode());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::HEADER_ROW_COUNT:
                    {
                        buffer.put_int64(GetRowsAffected());

                        return sql_result::AI_SUCCESS;
                    }

                    default:
                        break;
                }

                if (recNum < 1 || static_cast<size_t>(recNum) > statusRecords.size())
                    return sql_result::AI_NO_DATA;

                // Status record.
                const DiagnosticRecord& record = GetStatusRecord(recNum);

                switch (field)
                {
                    case diagnostic_field::STATUS_CLASS_ORIGIN:
                    {
                        buffer.put_string(record.GetClassOrigin());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_COLUMN_NUMBER:
                    {
                        buffer.put_int32(record.GetColumnNumber());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_CONNECTION_NAME:
                    {
                        buffer.put_string(record.GetConnectionName());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_MESSAGE_TEXT:
                    {
                        buffer.put_string(record.GetMessageText());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_NATIVE:
                    {
                        buffer.put_int32(0);

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_ROW_NUMBER:
                    {
                        buffer.put_int64(record.GetRowNumber());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_SERVER_NAME:
                    {
                        buffer.put_string(record.GetServerName());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_SQLSTATE:
                    {
                        buffer.put_string(record.Getsql_state());

                        return sql_result::AI_SUCCESS;
                    }

                    case diagnostic_field::STATUS_SUBCLASS_ORIGIN:
                    {
                        buffer.put_string(record.GetSubclassOrigin());

                        return sql_result::AI_SUCCESS;
                    }

                    default:
                        break;
                }

                return sql_result::AI_ERROR;
            }

        }
    }
}
