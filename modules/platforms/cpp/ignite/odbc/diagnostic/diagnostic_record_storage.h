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

#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/diagnostic/diagnostic_record.h"

#include <cstdint>
#include <vector>

namespace ignite {

/**
 * Diagnostic record storage.
 *
 * Associated with each environment, connection, statement, and descriptor handle are diagnostic records. These records
 * contain diagnostic information about the last function called that used a particular handle. The records are replaced
 * only when another function is called using that handle. There is no limit to the number of diagnostic records that
 * can be stored at any one time.
 *
 * This class provides interface for interaction with all handle diagnostic records. That means both header and status
 * records.
 */
class diagnostic_record_storage {
public:
    // Default
    diagnostic_record_storage() = default;

    // Deleted
    diagnostic_record_storage(diagnostic_record_storage &&) = delete;
    diagnostic_record_storage(const diagnostic_record_storage &) = delete;
    diagnostic_record_storage &operator=(diagnostic_record_storage &&) = delete;
    diagnostic_record_storage &operator=(const diagnostic_record_storage &) = delete;

    /**
     * Set header record values.
     *
     * @param result Operation return code.
     */
    void set_header_record(sql_result result);

    /**
     * Add new status record.
     *
     * @param sql_state SQL state.
     * @param message Message.
     */
    void add_status_record(sql_state sql_state, const std::string &message);

    /**
     * Add status record to diagnostic records.
     *
     * TODO: Replace with move
     * @param record Status record.
     */
    void add_status_record(const diagnostic_record &record);

    /**
     * reset diagnostic records state.
     */
    void reset();

    /**
     * Get result of the last operation.
     *
     * @return Result of the last operation.
     */
    [[nodiscard]] sql_result get_operation_result() const;

    /**
     * Get return code of the last operation.
     *
     * @return Return code of the last operation.
     */
    [[nodiscard]] SQLRETURN get_return_code() const;

    /**
     * Get row count.
     *
     * @return Count of rows in cursor.
     */
    [[nodiscard]] int64_t get_row_count() const;

    /**
     * Get dynamic function.
     *
     * @return String that describes the SQL statement that the underlying function executed.
     */
    [[nodiscard]] const std::string &get_dynamic_function() const;

    /**
     * Get dynamic function code.
     *
     * @return Numeric code that describes the SQL statement that was executed.
     */
    [[nodiscard]] int32_t get_dynamic_function_code() const;

    /**
     * Get number of rows affected.
     *
     * @return The number of rows affected by an insert, delete, or update performed by the last operation.
     */
    [[nodiscard]] int32_t get_rows_affected() const;

    /**
     * Get status records number.
     *
     * @return Number of status records.
     */
    [[nodiscard]] int32_t get_status_records_number() const;

    /**
     * Get specified status record.
     *
     * @param idx Status record index.
     * @return Status record instance reference.
     */
    [[nodiscard]] const diagnostic_record &get_status_record(int32_t idx) const;

    /**
     * Get specified status record.
     *
     * @param idx Status record index.
     * @return Status record instance reference.
     */
    diagnostic_record &get_status_record(int32_t idx);

    /**
     * Get last non-retrieved status record index.
     *
     * @return Index of the last non-retrieved status record or zero
     *  if nothing was found.
     */
    [[nodiscard]] int32_t get_last_non_retrieved() const;

    /**
     * Check if the record is in the success state.
     *
     * @return True if the record is in the success state.
     */
    [[nodiscard]] bool is_successful() const;

    /**
     * Get value of the field and put it in buffer.
     *
     * @param rec_num Diagnostic record number.
     * @param field Record field.
     * @param buffer Buffer to put data to.
     * @return Operation result.
     */
    sql_result get_field(int32_t rec_num, diagnostic_field field, application_data_buffer &buffer) const;

private:
    /** Header record field. This field contains the count of rows in the cursor. */
    int64_t m_row_count{0};

    /** Header record field. String that describes the SQL statement that the underlying function executed. */
    std::string m_dynamic_function;

    /** Header record field. Numeric code that describes the SQL statement that was executed. */
    int32_t m_dynamic_function_code{0};

    /** Operation result. This field is mapped to "Return code" header record field. */
    sql_result m_result{sql_result::AI_SUCCESS};

    /**
     * Header record field. The number of rows affected by an insert, delete, or update performed by the last operation.
     */
    int32_t m_rows_affected{0};

    /** Status records. */
    std::vector<diagnostic_record> m_status_records;
};

} // namespace ignite
