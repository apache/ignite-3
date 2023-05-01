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

#include "diagnosable_adapter.h"
#include "../connection.h"
#include "../log.h"
#include "../odbc_error.h"

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            void DiagnosableAdapter::AddStatusRecord(sql_state  sqlState,
                const std::string& message, int32_t rowNum, int32_t columnNum)
            {
                LOG_MSG("Adding new record: " << message << ", rowNum: " << rowNum << ", columnNum: " << columnNum);

                if (connection)
                {
                    diagnosticRecords.AddStatusRecord(
                        connection->CreateStatusRecord(sqlState, message, rowNum, columnNum));
                }
                else
                {
                    diagnosticRecords.AddStatusRecord(
                        DiagnosticRecord(sqlState, message, "", "", rowNum, columnNum));
                }
            }

            void DiagnosableAdapter::AddStatusRecord(sql_state  sqlState, const std::string& message)
            {
                AddStatusRecord(sqlState, message, 0, 0);
            }

            void DiagnosableAdapter::AddStatusRecord(const std::string& message)
            {
                AddStatusRecord(sql_state::SHY000_GENERAL_ERROR, message);
            }

            void DiagnosableAdapter::AddStatusRecord(const odbc_error& err)
            {
                AddStatusRecord(err.get_state(), err.get_error_message(), 0, 0);
            }

            void DiagnosableAdapter::AddStatusRecord(const DiagnosticRecord& rec)
            {
                diagnosticRecords.AddStatusRecord(rec);
            }
        }
    }
}

