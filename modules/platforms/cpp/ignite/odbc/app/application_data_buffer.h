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

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/type_traits.h"

#include <ignite/common/big_decimal.h>
#include <ignite/common/bit_array.h>
#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_date.h>
#include <ignite/common/ignite_date_time.h>
#include <ignite/common/ignite_duration.h>
#include <ignite/common/ignite_period.h>
#include <ignite/common/ignite_time.h>
#include <ignite/common/ignite_timestamp.h>
#include <ignite/common/uuid.h>

#include <cstdint>
#include <map>

namespace ignite {

/**
 * Conversion result
 */
enum class conversion_result {
    /** Conversion successful. No data lost. */
    AI_SUCCESS,

    /** Conversion successful, but fractional truncation occurred. */
    AI_FRACTIONAL_TRUNCATED,

    /** Conversion successful, but right-side variable length data truncation occurred. */
    AI_VARLEN_DATA_TRUNCATED,

    /** Conversion is not supported. */
    AI_UNSUPPORTED_CONVERSION,

    /** Indicator buffer needed to complete the operation but it is NULL. */
    AI_INDICATOR_NEEDED,

    /** No data found. */
    AI_NO_DATA,

    /** General operation failure. */
    AI_FAILURE
};

/**
 * User application data buffer.
 */
class application_data_buffer {
public:
    // Default
    application_data_buffer() = default;

    /**
     * Constructor.
     *
     * @param type Underlying data type.
     * @param buffer Data buffer pointer.
     * @param buf_len Data buffer length.
     * @param res_len Resulting data length.
     */
    application_data_buffer(odbc_native_type type, void *buffer, SQLLEN buf_len, SQLLEN *res_len);

    /**
     * Set offset in bytes for all bound pointers.
     *
     * @param offset Offset.
     */
    void set_byte_offset(int offset) { this->m_byte_offset = offset; }

    /**
     * Set offset in elements for all bound pointers.
     *
     * @param offset Offset.
     */
    void set_element_offset(SQLULEN offset) { this->m_element_offset = offset; }

    /**
     * Put in buffer value of type int8_t.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_int8(int8_t value);

    /**
     * Put in buffer value of type int16_t.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_int16(int16_t value);

    /**
     * Put in buffer value of type int32_t.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_int32(int32_t value);

    /**
     * Put in buffer value of type int64_t.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_int64(int64_t value);

    /**
     * Put in buffer value of type float.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_float(float value);

    /**
     * Put in buffer value of type double.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_double(double value);

    /**
     * Put in buffer value of type bool.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_bool(bool value);

    /**
     * Put in buffer value of type string.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_string(const std::optional<std::string> &value) {
        return value ? put_string(*value) : put_null();
    }

    /**
     * Put in buffer value of type string.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_string(const char *value) { return put_string(std::string(value)); }

    /**
     * Put in buffer value of type string.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_string(const std::string &value);

    /**
     * Put in buffer value of type string.
     *
     * @param value Value.
     * @param written Number of written characters.
     * @return Conversion result.
     */
    conversion_result put_string(const std::string &value, int32_t &written);

    /**
     * Put in buffer value of type GUID.
     *
     * @param value Value.
     * @return Conversion result.
     */
    conversion_result put_uuid(const uuid &value);

    /**
     * Put binary data in buffer.
     *
     * @param data Data pointer.
     * @param len Data length.
     * @param written Number of written characters.
     * @return Conversion result.
     */
    conversion_result put_binary_data(void *data, size_t len, std::int32_t &written);

    /**
     * Put binary data in buffer.
     *
     * @param data Data.
     * @return Conversion result.
     */
    conversion_result put_binary_data(bytes_view data) {
        std::int32_t dummy;
        return put_binary_data((void *) data.data(), data.size(), dummy);
    }

    /**
     * Put bitmask in buffer.
     *
     * @param data Data.
     * @return Conversion result.
     */
    conversion_result put_bitmask(const bit_array &data) { return put_binary_data(data.get_raw()); }

    /**
     * Put NULL.
     * @return Conversion result.
     */
    conversion_result put_null();

    /**
     * Put decimal value to buffer.
     *
     * @param value Value to put.
     * @return Conversion result.
     */
    conversion_result put_decimal(const big_decimal &value);

    /**
     * Put date to buffer.
     *
     * @param value Value to put.
     * @return Conversion result.
     */
    conversion_result put_date(const ignite_date &value);

    /**
     * Put timestamp to buffer.
     *
     * @param value Value to put.
     * @return Conversion result.
     */
    conversion_result put_timestamp(const ignite_timestamp &value);

    /**
     * Put time to buffer.
     *
     * @param value Value to put.
     * @return Conversion result.
     */
    conversion_result put_time(const ignite_time &value);

    /**
     * Put datetime to buffer.
     *
     * @param value Value to put.
     * @return Conversion result.
     */
    conversion_result put_date_time(const ignite_date_time &value);

    /**
     * Get string.
     *
     * @return String value of buffer.
     */
    [[nodiscard]] std::string get_string(size_t maxLen) const;

    /**
     * Get value of type int8_t.
     *
     * @return Integer value of type int8_t.
     */
    [[nodiscard]] int8_t get_int8() const;

    /**
     * Get value of type int16_t.
     *
     * @return Integer value of type int16_t.
     */
    [[nodiscard]] int16_t get_int16() const;

    /**
     * Get value of type int32_t.
     *
     * @return Integer value of type int32_t.
     */
    [[nodiscard]] int32_t get_int32() const;

    /**
     * Get value of type int64_t.
     *
     * @return Integer value of type int64_t.
     */
    [[nodiscard]] int64_t get_int64() const;

    /**
     * Get value of type float.
     *
     * @return Integer value of type float.
     */
    [[nodiscard]] float get_float() const;

    /**
     * Get value of type double.
     *
     * @return Value of type double.
     */
    [[nodiscard]] double get_double() const;

    /**
     * Get value of type GUID.
     *
     * @return Value of type uuid.
     */
    [[nodiscard]] uuid get_uuid() const;

    /**
     * Get value of type ignite_date.
     *
     * @return Value of type ignite_date.
     */
    [[nodiscard]] ignite_date get_date() const;

    /**
     * Get value of type ignite_date_time.
     *
     * @return Value of type ignite_date_time.
     */
    [[nodiscard]] ignite_date_time get_date_time() const;

    /**
     * Get value of type ignite_timestamp.
     *
     * @return Value of type ignite_timestamp.
     */
    [[nodiscard]] ignite_timestamp get_timestamp() const;

    /**
     * Get value of type ignite_time.
     *
     * @return Value of type ignite_timestamp.
     */
    [[nodiscard]] ignite_time get_time() const;

    /**
     * Get value of type big_decimal.
     *
     * @param val Result is placed here.
     */
    void get_decimal(big_decimal &val) const;

    /**
     * Get raw data.
     *
     * @return Buffer data.
     */
    [[nodiscard]] const void *get_data() const;

    /**
     * Get result data length.
     *
     * @return Data length pointer.
     */
    [[nodiscard]] const SQLLEN *get_result_len() const;

    /**
     * Get raw data.
     *
     * @return Buffer data.
     */
    void *get_data();

    /**
     * Get result data length.
     *
     * @return Data length pointer.
     */
    SQLLEN *get_result_len();

    /**
     * Get buffer size in bytes.
     *
     * @return Buffer size.
     */
    [[nodiscard]] SQLLEN get_size() const { return m_buffer_len; }

    /**
     * Check if the data is going to be provided at execution.
     *
     * @return True if the data is going to be provided
     *     at execution.
     */
    [[nodiscard]] bool is_data_at_exec() const;

    /**
     * Get size of the data that is going to be provided at
     * execution.
     *
     * @return Size of the data that is going to be provided
     *     at execution.
     */
    [[nodiscard]] SQLLEN get_data_at_exec_size() const;

    /**
     * Get single element size.
     *
     * @return Size of the single element.
     */
    [[nodiscard]] SQLLEN get_element_size() const;

    /**
     * Get size of the input buffer.
     *
     * @return Input buffer size, or zero if the data is going
     *     to be provided at execution.
     */
    [[nodiscard]] SQLLEN get_input_size() const;

    /**
     * Get buffer type.
     *
     * @return Buffer type.
     */
    [[nodiscard]] odbc_native_type get_type() const { return m_type; }

private:
    /**
     * Put value of numeric type in the buffer.
     *
     * @param value Numeric value to put.
     * @return Conversion result.
     */
    template<typename T>
    conversion_result put_num(T value);

    /**
     * Put numeric value to numeric buffer.
     *
     * @param value Numeric value.
     * @return Conversion result.
     */
    template<typename TBuf, typename TIn>
    conversion_result put_num_to_num_buffer(TIn value);

    /**
     * Put value to string buffer.
     *
     * @param value Value that can be converted to string.
     * @return Conversion result.
     */
    template<typename CharT, typename Tin>
    conversion_result put_value_to_string_buffer(const Tin &value);

    /**
     * Put value to string buffer.
     * Specialisation for int8_t.
     *
     * @param value Value that can be converted to string.
     * @return Conversion result.
     */
    template<typename CharT>
    conversion_result put_value_to_string_buffer(const int8_t &value);

    /**
     * Put string to string buffer.
     *
     * @param value String value.
     * @param written Number of characters written.
     * @return Conversion result.
     */
    template<typename OutCharT, typename InCharT>
    conversion_result put_string_to_string_buffer(const std::basic_string<InCharT> &value, int32_t &written);

    /**
     * Put raw data to any buffer.
     *
     * @param data Data pointer.
     * @param len Data length.
     * @param written Number of characters written.
     * @return Conversion result.
     */
    conversion_result put_raw_data_to_buffer(void *data, size_t len, int32_t &written);

    /**
     * Put data from struct tm to a string buffer.
     *
     * @param tm_time Time.
     * @param val_len Resulting string length.
     * @param fmt Format for underlying strftime.
     * @return Result.
     */
    conversion_result put_tm_to_string(tm &tm_time, SQLLEN val_len, const char *fmt);

    /**
     * Get int of type T.
     *
     * @return Integer value of specified type.
     */
    template<typename T>
    T get_num() const;

    /**
     * Apply buffer offset to pointer.
     * Adds offset to pointer if offset pointer is not null.
     *
     * @param ptr Pointer.
     * @param elemSize Element size.
     * @return Pointer with applied offset.
     */
    template<typename T>
    T *apply_offset(T *ptr, size_t elemSize) const;

    /** Underlying data type. */
    odbc_native_type m_type{odbc_native_type::AI_UNSUPPORTED};

    /** Buffer pointer. */
    void *m_buffer{nullptr};

    /** Buffer length. */
    SQLLEN m_buffer_len{0};

    /** Result length. */
    SQLLEN *m_result_len{nullptr};

    /** Current byte offset */
    int m_byte_offset{0};

    /** Current element offset. */
    SQLULEN m_element_offset{0};
};

/** Column binging map type alias. */
typedef std::map<std::uint16_t, application_data_buffer> column_binding_map;

} // namespace ignite
