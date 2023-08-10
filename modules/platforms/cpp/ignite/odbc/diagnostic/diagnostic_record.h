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

#include <cstdint>
#include <utility>
#include <vector>

namespace ignite {

/**
 * Status diagnostic record.
 */
class diagnostic_record {
public:
    // Default
    diagnostic_record() = default;

    /**
     * Constructor.
     *
     * @param sql_state SQL state code.
     * @param message Message.
     * @param connection_name Connection name.
     * @param server_name Server name.
     * @param row_num Associated row number.
     * @param column_num Associated column number.
     */
    diagnostic_record(sql_state sql_state, std::string message, std::string connection_name, std::string server_name,
        int32_t row_num = 0, int32_t column_num = 0)
        : m_sql_state(sql_state)
        , m_message(std::move(message))
        , m_connection_name(std::move(connection_name))
        , m_server_name(std::move(server_name))
        , m_row_num(row_num)
        , m_column_num(column_num)
        , m_retrieved(false) {}

    /**
     * Get class origin.
     *
     * @return A string that indicates the document that defines the class portion of the SQLSTATE value in this record.
     */
    [[nodiscard]] const std::string &get_class_origin() const;

    /**
     * Get subclass origin.
     *
     * @return A string with the same format and valid values as origin, that identifies the defining portion of the
     *     subclass portion of the SQLSTATE code.
     */
    [[nodiscard]] const std::string &get_subclass_origin() const;

    /**
     * Get record message text.
     *
     * @return An informational message on the error or warning.
     */
    [[nodiscard]] const std::string &get_message_text() const;

    /**
     * Get connection name.
     *
     * @return A string that indicates the name of the connection that the diagnostic record relates to.
     */
    [[nodiscard]] const std::string &get_connection_name() const;

    /**
     * Get server name.
     *
     * @return A string that indicates the server name that the diagnostic record relates to.
     */
    [[nodiscard]] const std::string &get_server_name() const;

    /**
     * Get SQL state of the record.
     *
     * @return A five-character SQLSTATE diagnostic code.
     */
    [[nodiscard]] const std::string &get_sql_state() const;

    /**
     * Get row number.
     *
     * @return The row number in the row set, or the parameter number in the set of m_parameters, with which the status
     *     record is associated.
     */
    [[nodiscard]] int32_t get_row_number() const;

    /**
     * Get column number.
     *
     * @return Contains the value that represents the column number in the result set or the parameter number in the set
     *     of m_parameters.
     */
    [[nodiscard]] int32_t get_column_number() const;

    /**
     * Check if the record was retrieved with the SQLError previously.
     *
     * return True if the record was retrieved with the SQLError previously.
     */
    [[nodiscard]] bool is_retrieved() const;

    /**
     * Mark record as retrieved with the SQLError.
     */
    void mark_retrieved();

private:
    /** SQL state diagnostic code. */
    sql_state m_sql_state{sql_state::UNKNOWN};

    /** An informational message on the error or warning. */
    std::string m_message{};

    /**
     * A string that indicates the name of the connection that
     * the diagnostic record relates to.
     */
    std::string m_connection_name{};

    /**
     * A string that indicates the server name that the
     * diagnostic record relates to.
     */
    std::string m_server_name{};

    /**
     * The row number in the row set, or the parameter number in the
     * set of m_parameters, with which the status record is associated.
     */
    int32_t m_row_num{0};

    /**
     * Contains the value that represents the column number in the
     * result set or the parameter number in the set of m_parameters.
     */
    int32_t m_column_num{0};

    /**
     * Flag that shows if the record was retrieved with the
     * SQLError previously.
     */
    bool m_retrieved{false};
};

} // namespace ignite
