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

#ifndef _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE_ADAPTER
#define _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE_ADAPTER

#include "ignite/odbc/diagnostic/diagnosable.h"

#define IGNITE_ODBC_API_CALL(...)                   \
        diagnosticRecords.reset();                  \
        sql_result result = (__VA_ARGS__);     \
        diagnosticRecords.set_header_record(result)

#define IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS                     \
        diagnosticRecords.reset();                              \
        diagnosticRecords.set_header_record(sql_result::AI_SUCCESS)

namespace ignite
{
    namespace odbc
    {
        class odbc_error;
        class Connection;

        namespace diagnostic
        {
            /**
             * Diagnosable interface.
             */
            class DiagnosableAdapter : public Diagnosable
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param connection Pointer to connection. Used to create
                 *     diagnostic records with connection info.
                 */
                DiagnosableAdapter(const Connection* connection = 0) :
                    connection(connection)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~DiagnosableAdapter()
                {
                    // No-op.
                }

                /**
                 * Get diagnostic record.
                 *
                 * @return Diagnostic record.
                 */
                virtual const diagnostic_record_storage& GetDiagnosticRecords() const
                {
                    return diagnosticRecords;
                }

                /**
                 * Get diagnostic record.
                 *
                 * @return Diagnostic record.
                 */
                virtual diagnostic_record_storage& GetDiagnosticRecords()
                {
                    return diagnosticRecords;
                }

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 * @param row_num Associated row number.
                 * @param columnNum Associated column number.
                 */
                virtual void add_status_record(sql_state  sqlState, const std::string& message,
                    int32_t row_num, int32_t columnNum);

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 */
                virtual void add_status_record(sql_state  sqlState, const std::string& message);

                /**
                 * Add new status record with sql_state::SHY000_GENERAL_ERROR state.
                 *
                 * @param message Message.
                 */
                virtual void add_status_record(const std::string& message);

                /**
                 * Add new status record.
                 *
                 * @param err Error.
                 */
                virtual void add_status_record(const odbc_error& err);

                /**
                 * Add new status record.
                 *
                 * @param rec Record.
                 */
                virtual void add_status_record(const diagnostic_record& rec);

            protected:
                /** Diagnostic records. */
                diagnostic_record_storage diagnosticRecords;

            private:
                /** Connection. */
                const Connection* connection;
            };
        }
    }
}

#endif //_IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE_ADAPTER
