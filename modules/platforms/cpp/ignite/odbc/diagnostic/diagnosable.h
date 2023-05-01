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

#pragma once

#include "ignite/odbc/diagnostic/diagnostic_record_storage.h"

namespace ignite::odbc
{
class odbc_error;

/**
 * Diagnosable interface.
 */
class Diagnosable
{
public:
    /**
     * Destructor.
     */
    virtual ~Diagnosable()
    {
        // No-op.
    }

    /**
     * Get diagnostic record.
     *
     * @return Diagnostic record.
     */
    virtual const diagnostic_record_storage& GetDiagnosticRecords() const = 0;

    /**
     * Get diagnostic record.
     *
     * @return Diagnostic record.
     */
    virtual diagnostic_record_storage& GetDiagnosticRecords() = 0;

    /**
     * Add new status record.
     *
     * @param sqlState SQL state.
     * @param message Message.
     * @param row_num Associated row number.
     * @param columnNum Associated column number.
     */
    virtual void AddStatusRecord(sql_state sqlState, const std::string& message,
        int32_t row_num, int32_t columnNum) = 0;

    /**
     * Add new status record.
     *
     * @param sqlState SQL state.
     * @param message Message.
     */
    virtual void AddStatusRecord(sql_state sqlState, const std::string& message) = 0;

    /**
     * Add new status record.
     *
     * @param err Error.
     */
    virtual void AddStatusRecord(const odbc_error& err) = 0;

    /**
     * Add new status record.
     *
     * @param rec Record.
     */
    virtual void AddStatusRecord(const diagnostic_record& rec) = 0;

protected:
    /**
     * Default constructor.
     */
    Diagnosable()
    {
        // No-op.
    }
};

} // namespace ignite
