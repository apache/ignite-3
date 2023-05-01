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

#include <cstdint>

#include <map>

#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/protocol/writer.h"

namespace ignite {

/**
 * Statement parameter.
 */
class parameter
{
public:
    // Default;
    parameter() = default;

    /**
     * Constructor.
     *
     * @param buffer Underlying data buffer.
     * @param sql_type IPD type.
     * @param column_size IPD column size.
     * @param dec_digits IPD decimal digits.
     */
    parameter(const application_data_buffer& buffer, int16_t sql_type, size_t column_size, int16_t dec_digits)
        : m_buffer(buffer)
        , m_sql_type(sql_type)
        , m_column_size(column_size)
        , m_dec_digits(dec_digits) { }

    /**
     * Write parameter using provided writer.
     *
     * @param writer Writer.
     * @param offset Offset for the buffer.
     * @param idx Index for the array-of-m_parameters case.
     */
    void write(protocol::writer& writer, int offset = 0, SQLULEN idx = 0) const;

    /**
     * Get data buffer.
     *
     * @return underlying application_data_buffer instance.
     */
    application_data_buffer& get_buffer();

    /**
     * Get data buffer.
     *
     * @return underlying application_data_buffer instance.
     */
    [[nodiscard]] const application_data_buffer& get_buffer() const;

    /**
     * Reset stored at-execution data.
     */
    void reset_stored_data();

    /**
     * Check if all the at-execution data has been stored.
     * @return
     */
    [[nodiscard]] bool is_data_ready() const;

    /**
     * Put at-execution data.
     *
     * @param data Data buffer pointer.
     * @param len Data length.
     */
    void put_data(void* data, SQLLEN len);

private:
    /** Underlying data buffer. */
    application_data_buffer m_buffer{};

    /** IPD type. */
    int16_t m_sql_type{0};

    /** IPD column size. */
    size_t m_column_size{0};

    /** IPD decimal digits. */
    int16_t m_dec_digits{0};

    /** User provided null data at execution. */
    bool m_null_data{false};

    /** Stored at-execution data. */
    std::vector<int8_t> m_stored_data;
};

} // namespace ignite
