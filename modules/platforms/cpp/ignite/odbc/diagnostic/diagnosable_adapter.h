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

#include "ignite/odbc/diagnostic/diagnosable.h"

#define IGNITE_ODBC_API_CALL(...)                                                                                      \
 m_diagnostic_records.reset();                                                                                         \
 sql_result result = (__VA_ARGS__);                                                                                    \
 m_diagnostic_records.set_header_record(result)

#define IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS                                                                            \
 m_diagnostic_records.reset();                                                                                         \
 m_diagnostic_records.set_header_record(sql_result::AI_SUCCESS)

namespace ignite {
class odbc_error;

/**
 * Diagnosable interface.
 */
class diagnosable_adapter : public diagnosable {
public:
    // Default
    diagnosable_adapter() = default;

    /**
     * Get diagnostic record.
     *
     * @return Diagnostic record.
     */
    [[nodiscard]] const diagnostic_record_storage &get_diagnostic_records() const override {
        return m_diagnostic_records;
    }

    /**
     * Get diagnostic record.
     *
     * @return Diagnostic record.
     */
    [[nodiscard]] diagnostic_record_storage &get_diagnostic_records() override { return m_diagnostic_records; }

    /**
     * Add new status record.
     *
     * @param sql_state SQL state.
     * @param message Message.
     * @param row_num Associated row number.
     * @param column_num Associated column number.
     */
    void add_status_record(
        sql_state sql_state, const std::string &message, int32_t row_num, int32_t column_num) override;

    /**
     * Add new status record.
     *
     * @param sql_state SQL state.
     * @param message Message.
     */
    void add_status_record(sql_state sql_state, const std::string &message) override;

    /**
     * Add new status record with sql_state::SHY000_GENERAL_ERROR state.
     *
     * @param message Message.
     */
    void add_status_record(const std::string &message);

    /**
     * Add new status record.
     *
     * @param err Error.
     */
    void add_status_record(const odbc_error &err) override;

    /**
     * Add new status record.
     *
     * @param rec Record.
     */
    void add_status_record(const diagnostic_record &rec) override;

protected:
    /** Diagnostic records. */
    diagnostic_record_storage m_diagnostic_records;
};

} // namespace ignite
