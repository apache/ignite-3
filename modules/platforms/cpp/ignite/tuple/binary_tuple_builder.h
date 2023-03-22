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

#include "binary_tuple_schema.h"

#include <ignite/common/big_decimal.h>
#include <ignite/common/big_integer.h>
#include <ignite/common/bytes.h>
#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_date.h>
#include <ignite/common/ignite_date_time.h>
#include <ignite/common/ignite_duration.h>
#include <ignite/common/ignite_period.h>
#include <ignite/common/ignite_time.h>
#include <ignite/common/ignite_timestamp.h>
#include <ignite/common/uuid.h>

#include <cassert>
#include <cstring>
#include <limits>
#include <type_traits>

namespace ignite {

/**
 * @brief Binary tuple builder.
 *
 * A tuple builder is used to create one or more binary tuples for a given schema.
 *
 * Building a tuple takes two passes. On the first pass the builder finds the tuple
 * layout. On the second pass it actually fills the tuple data.
 *
 * More precisely a tuple is built with the following call sequence:
 *
 * 1. Initialize the builder with the @ref start call.
 * 2. Supply all elements with one or more @ref claim calls in the order that
 *    corresponds to the tuple schema.
 * 3. Determine tuple layout with the @ref layout call.
 * 4. Supply all elements again with one or more @ref append calls in the same
 *    order with the same values.
 * 5. Finally, the resulting binary tuple is obtained with the @ref build call.
 */
class binary_tuple_builder {
    const number_t element_count; /**< Total number of elements. */

    number_t element_index; /**< Index of the next element to add. */

    number_t null_elements; /**< The number of null elements. */

    data_size_t value_area_size; /**< Total size of all values. */

    data_size_t entry_size; /**< Size of an offset table entry. */

    std::byte *next_entry; /**< Position for the next offset table entry. */

    std::byte *value_base; /**< Position of the value area.*/

    std::byte *next_value; /**< Position for the next value. */

    std::vector<std::byte> binary_tuple; /**< Internal buffer for tuple generation. */

public:
    /**
     * @brief Constructs a new Tuple Builder object.
     *
     * @param schema Binary tuple schema.
     */
    explicit binary_tuple_builder(number_t element_count) noexcept;

    /**
     * @brief Starts a new tuple.
     */
    void start() noexcept;

    /**
     * @brief Assigns a null value for the next element.
     */
    void claim(std::nullopt_t /*null*/) noexcept {
        assert(element_index < element_count);
        null_elements++;
        element_index++;
    }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param size required size for the value
     */
    void claim(data_size_t size) noexcept {
        assert(element_index < element_count);
        value_area_size += size;
        element_index++;
    }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_bool(bool value) noexcept { claim(gauge_bool(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_int8(std::int8_t value) noexcept { claim(gauge_int8(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_int16(std::int16_t value) noexcept { claim(gauge_int16(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_int32(std::int32_t value) noexcept { claim(gauge_int32(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_int64(std::int64_t value) noexcept { claim(gauge_int64(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_float(float value) noexcept { claim(gauge_float(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_double(double value) noexcept { claim(gauge_double(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_uuid(uuid value) noexcept { claim(gauge_uuid(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_number(const big_integer &value) noexcept { claim(gauge_number(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_number(const big_decimal &value) noexcept { claim(gauge_number(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_date(const ignite_date &value) noexcept { claim(gauge_date(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_time(const ignite_time &value) noexcept { claim(gauge_time(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_date_time(const ignite_date_time &value) noexcept { claim(gauge_date_time(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_timestamp(const ignite_timestamp &value) noexcept { claim(gauge_timestamp(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_period(const ignite_period &value) noexcept { claim(gauge_period(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_duration(const ignite_duration &value) noexcept { claim(gauge_duration(value)); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_string(const std::string &value) noexcept { claim(data_size_t(value.size())); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_bytes(const bytes_view &value) noexcept { claim(data_size_t(value.size())); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param type Element type.
     * @param bytes Binary element value.
     */
    void claim(ignite_type type, const bytes_view &bytes) noexcept { claim(gauge(type, bytes)); }

    /**
     * @brief Assigns a value or null for the next element.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param type Element type.
     * @param slice Optional value of an internal tuple field.
     */
    template<typename BytesT>
    void claim(ignite_type type, const std::optional<BytesT> &slice) noexcept {
        if (slice.has_value()) {
            claim(type, slice.value());
        } else {
            claim(std::nullopt);
        }
    }

    /**
     * @brief Assigns values for a number of elements.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param schema Tuple schema.
     * @param tuple Tuple in the internal form.
     */
    template<typename BytesT>
    void claim(const binary_tuple_schema &schema, const std::vector<std::optional<BytesT>> &tuple) noexcept {
        for (number_t i = 0; i < schema.num_elements(); i++) {
            claim(schema.get_element(i).type, tuple[i]);
        }
    }

    /**
     * @brief Performs binary tuple layout.
     */
    void layout();

    /**
     * @brief Appends a null value for the next element.
     */
    void append(std::nullopt_t /*null*/) {
        assert(null_elements > 0);
        assert(element_index < element_count);
        binary_tuple[binary_tuple_schema::get_null_offset(element_index)] |=
            binary_tuple_schema::get_null_mask(element_index);
        append_entry();
    }

    /**
     * @brief Appends a value for the next element.
     *
     * @param type Element type.
     * @param value Value of an internal tuple field.
     */
    void append(ignite_type type, const bytes_view &bytes);

    /**
     * @brief Appends a value or null for the next element.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param type Element type.
     * @param slice Optional value of an internal tuple field.
     */
    template<typename BytesT>
    void append(ignite_type type, const std::optional<BytesT> &slice) {
        if (slice.has_value()) {
            append(type, slice.value());
        } else {
            append(std::nullopt);
        }
    }

    /**
     * @brief Appends values for a number of elements.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param schema Tuple schema.
     * @param tuple Tuple in the internal form.
     */
    template<typename BytesT>
    void append(const binary_tuple_schema &schema, const std::vector<std::optional<BytesT>> &tuple) {
        for (number_t i = 0; i < schema.num_elements(); i++) {
            append(schema.get_element(i).type, tuple[i]);
        }
    }

    /**
     * @brief Writes binary value of specified element.
     *
     * @param bytes Binary element value.
     */
    void append_bytes(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_bool(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_bool(bool value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_int8(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_int8(std::int8_t value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_int16(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_int16(std::int16_t value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_int32(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_int32(std::int32_t value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_int64(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_int64(std::int64_t value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_float(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_float(float value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_double(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_double(double value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_number(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Big integer value.
     */
    void append_number(const big_integer &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Big decimal value.
     */
    void append_number(const big_decimal &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_uuid(bytes_view bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_uuid(uuid value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_date(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Date value.
     */
    void append_date(const ignite_date &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_time(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Time value.
     */
    void append_time(const ignite_time &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_date_time(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Date time value.
     */
    void append_date_time(const ignite_date_time &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_timestamp(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Timestamp value.
     */
    void append_timestamp(const ignite_timestamp &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_period(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Time value.
     */
    void append_period(const ignite_period &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_duration(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Time value.
     */
    void append_duration(const ignite_duration &value);

    /**
     * @brief Appends a string as the next element.
     *
     * @param value Element value.
     */
    void append_string(const std::string &value) {
        append_bytes({reinterpret_cast<const std::byte *>(value.data()), value.size()});
    }

    /**
     * @brief Finalizes and returns a binary tuple.
     *
     * @return Byte buffer with binary tuple.
     */
    const std::vector<std::byte> &build() {
        assert(element_index == element_count);
        return binary_tuple;
    }

    /**
     * @brief Builds a binary tuple from an internal tuple representation.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param schema Tuple schema.
     * @param tuple Tuple in the internal form.
     * @return Byte buffer with binary tuple.
     */
    template<typename BytesT>
    const std::vector<std::byte> &build(
        const binary_tuple_schema &schema, const std::vector<std::optional<BytesT>> &tuple) {
        start();
        claim(schema, tuple);
        layout();
        append(schema, tuple);
        return build();
    }

private:
    /**
     * @brief Checks if a value of a given integer type can be compressed to a smaller integer type.
     *
     * @tparam TGT Target integer type.
     * @tparam SRC Source integer type.
     * @param value Source value.
     * @return true If the source value can be compressed.
     * @return false If the source value cannot be compressed.
     */
    template<typename TGT, typename SRC>
    static bool fits(SRC value) noexcept {
        static_assert(std::is_signed_v<SRC>);
        static_assert(std::is_signed_v<TGT>);
        static_assert(sizeof(TGT) < sizeof(SRC));
        // Check if TGT::min <= value <= TGT::max.
        return std::make_unsigned_t<SRC>(value + std::numeric_limits<TGT>::max() + 1)
            <= std::numeric_limits<std::make_unsigned_t<TGT>>::max();
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_bool(bool value) noexcept { return !value ? 0 : sizeof(std::int8_t); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_int8(std::int8_t value) noexcept { return value == 0 ? 0 : sizeof(std::int8_t); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_int16(std::int16_t value) noexcept {
        if (fits<std::int8_t>(value)) {
            return gauge_int8(std::int8_t(value));
        }
        return sizeof(std::int16_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_int32(std::int32_t value) noexcept {
        if (fits<std::int8_t>(value)) {
            return gauge_int8(std::int8_t(value));
        }
        if (fits<std::int16_t>(value)) {
            return sizeof(std::int16_t);
        }
        return sizeof(std::int32_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_int64(std::int64_t value) noexcept {
        if (fits<std::int16_t>(value)) {
            return gauge_int16(std::int16_t(value));
        }
        if (fits<std::int32_t>(value)) {
            return sizeof(std::int32_t);
        }
        return sizeof(std::int64_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_float(float value) noexcept { return value == 0.0f ? 0 : sizeof(float); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_double(double value) noexcept {
        auto floatValue = static_cast<float>(value);
        return floatValue == value ? gauge_float(floatValue) : sizeof(double);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_number(const big_integer &value) noexcept {
        return data_size_t(value.is_zero() ? 0 : value.byte_size());
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_number(const big_decimal &value) noexcept {
        return data_size_t(value.is_zero() ? 0 : value.get_unscaled_value().byte_size());
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_uuid(const uuid &value) noexcept { return value == uuid() ? 0 : 16; }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_date(const ignite_date &value) noexcept { return value == ignite_date() ? 0 : 3; }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_time(const ignite_time &value) noexcept {
        if (value == ignite_time()) {
            return 0;
        }

        auto nanos = value.get_nano();
        if ((nanos % 1000) != 0) {
            return 6;
        } else if ((nanos % 1000000) != 0) {
            return 5;
        }
        return 4;
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_date_time(const ignite_date_time &value) noexcept {
        if (value == ignite_date_time()) {
            return 0;
        }

        auto nanos = value.get_nano();
        if ((nanos % 1000) != 0) {
            return 9;
        } else if ((nanos % 1000000) != 0) {
            return 8;
        }
        return 7;
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_timestamp(const ignite_timestamp &value) noexcept {
        return value == ignite_timestamp() ? 0 : value.get_nano() == 0 ? 8 : 12;
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_period(const ignite_period &value) noexcept {
        const auto y = value.get_years();
        const auto m = value.get_months();
        const auto d = value.get_days();
        if (fits<std::int8_t>(y) && fits<std::int8_t>(m) && fits<std::int8_t>(d)) {
            if (value == ignite_period()) {
                return 0;
            }
            return 3;
        }
        if (fits<std::int16_t>(y) && fits<std::int16_t>(m) && fits<std::int16_t>(d)) {
            return 6;
        }
        return 12;
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static data_size_t gauge_duration(const ignite_duration &value) noexcept {
        return value == ignite_duration() ? 0 : value.get_nano() == 0 ? 8 : 12;
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param type Element type.
     * @param bytes Binary element value.
     * @return Required size.
     */
    static data_size_t gauge(ignite_type type, bytes_view bytes);

    /**
     * @brief Adds an entry to the offset table.
     */
    void append_entry() {
        auto offset = bytes::htol<std::ptrdiff_t>(next_value - value_base);
        std::memcpy(next_entry, &offset, entry_size);
        next_entry += entry_size;
        element_index++;
    }
};

} // namespace ignite
