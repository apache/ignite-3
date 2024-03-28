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

#include "binary_tuple_common.h"

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
#include <ignite/common/ignite_type.h>
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
protected:
    const tuple_num_t element_count; /**< Total number of elements. */

    tuple_num_t element_index; /**< Index of the next element to add. */

    tuple_size_t value_area_size; /**< Total size of all values. */

    tuple_size_t entry_size; /**< Size of an offset table entry. */

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
    explicit binary_tuple_builder(tuple_num_t element_count) noexcept;

    /**
     * @brief Starts a new tuple.
     */
    void start() noexcept;

    /**
     * @brief Assigns a null value for the next element.
     */
    void claim_null() noexcept { claim(0); }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param value Element value.
     */
    void claim_varlen(const bytes_view &value) noexcept {
        auto size = value.size();
        if (size == 0 || value[0] == binary_tuple_common::VARLEN_EMPTY_BYTE) {
            size++;
        }
        claim(tuple_size_t(size));
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
     * @brief Performs binary tuple layout.
     */
    void layout();

    /**
     * @brief Appends a null value as the next element.
     */
    void append_null() {
        assert(element_index < element_count);
        append_entry();
    }

    /**
     * @brief Writes binary value of specified element.
     *
     * @param bytes Binary element value.
     */
    void append_varlen(bytes_view bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void append_bool(bool value);

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
    void append_int8_ptr(std::int8_t *bytes);

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
    void append_int16_ptr(std::int16_t *bytes);

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
    void append_int32_ptr(std::int32_t *bytes);

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
    void append_int64_ptr(std::int64_t *bytes);

    /**
     * @brief Writes specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param value Element value.
     */
    void append_float(float value);

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
     * @param value Big integer value.
     */
    void append_number(const big_integer &value);

    /**
     * @brief Writes binary value of specified element.
     *
     * @param value Big decimal value.
     */
    void append_number(const big_decimal &value);

    /**
     * @brief Writes specified element.
     *
     * @param value Element value.
     */
    void append_uuid(uuid value);

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
     * @param value Time value.
     */
    void append_time(const ignite_time &value);

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
     * @param value Timestamp value.
     */
    void append_timestamp(const ignite_timestamp &value);

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
     * @param value Time value.
     */
    void append_duration(const ignite_duration &value);

    /**
     * @brief Finalizes and returns a binary tuple.
     *
     * @return Byte buffer with binary tuple.
     */
    const std::vector<std::byte> &build() {
        assert(element_index == element_count);
        return binary_tuple;
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
    static tuple_size_t gauge_bool(bool /*value*/) noexcept { return sizeof(std::int8_t); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_int8(std::int8_t /*value*/) noexcept { return sizeof(std::int8_t); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_int16(std::int16_t value) noexcept {
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
    static tuple_size_t gauge_int32(std::int32_t value) noexcept {
        if (fits<std::int16_t>(value)) {
            return gauge_int16(std::int16_t(value));
        }
        return sizeof(std::int32_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_int64(std::int64_t value) noexcept {
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
    static tuple_size_t gauge_float(float /*value*/) noexcept { return sizeof(float); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_double(double value) noexcept {
        const auto float_value = static_cast<float>(value);
        return float_value == value ? gauge_float(float_value) : sizeof(double);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_number(const big_integer &value) noexcept { return tuple_size_t(value.byte_size()); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_number(const big_decimal &value) noexcept {
        return tuple_size_t(2 + value.get_unscaled_value().byte_size());
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_uuid(const uuid & /*value*/) noexcept { return 16; }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_date(const ignite_date & /*value*/) noexcept { return 3; }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_time(const ignite_time &value) noexcept {
        auto nanos = value.get_nano();
        if ((nanos % 1000) != 0) {
            return 6;
        }
        if ((nanos % 1000000) != 0) {
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
    static tuple_size_t gauge_date_time(const ignite_date_time &value) noexcept {
        auto nanos = value.get_nano();
        if ((nanos % 1000) != 0) {
            return 9;
        }
        if ((nanos % 1000000) != 0) {
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
    static tuple_size_t gauge_timestamp(const ignite_timestamp &value) noexcept {
        return value.get_nano() == 0 ? 8 : 12;
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static tuple_size_t gauge_period(const ignite_period &value) noexcept {
        const auto y = value.get_years();
        const auto m = value.get_months();
        const auto d = value.get_days();
        if (fits<std::int8_t>(y) && fits<std::int8_t>(m) && fits<std::int8_t>(d)) {
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
    static tuple_size_t gauge_duration(const ignite_duration &value) noexcept { return value.get_nano() == 0 ? 8 : 12; }

    /**
     * @brief Reserves space for the next element.
     *
     * @param size Required size for the element.
     */
    void claim(tuple_size_t size) noexcept {
        assert(element_index < element_count);
        value_area_size += size;
        element_index++;
    }

    /**
     * @brief Writes binary value of specified element.
     *
     * @param bytes Binary element value.
     */
    void append_bytes(bytes_view bytes);

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
