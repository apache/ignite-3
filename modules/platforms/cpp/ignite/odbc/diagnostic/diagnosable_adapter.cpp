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
            void DiagnosableAdapter::add_status_record(sql_state  sqlState,
                const std::string& message, int32_t row_num, int32_t columnNum)
            {
                LOG_MSG("Adding new record: " << message << ", row_num: " << row_num << ", columnNum: " << columnNum);

                if (connection)
                {
                    diagnosticRecords.add_status_record(
                        connection->CreateStatusRecord(sqlState, message, row_num, columnNum));
                }
                else
                {
                    diagnosticRecords.add_status_record(
                        diagnostic_record(sqlState, message, "", "", row_num, columnNum));
                }
            }

            void DiagnosableAdapter::add_status_record(sql_state  sqlState, const std::string& message)
            {
                add_status_record(sqlState, message, 0, 0);
            }

            void DiagnosableAdapter::add_status_record(const std::string& message)
            {
                add_status_record(sql_state::SHY000_GENERAL_ERROR, message);
            }

            void DiagnosableAdapter::add_status_record(const odbc_error& err)
            {
                add_status_record(err.get_state(), err.get_error_message(), 0, 0);
            }

            void DiagnosableAdapter::add_status_record(const diagnostic_record& rec)
            {
                diagnosticRecords.add_status_record(rec);
            }
        }
    }
}

