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

#include "binary_tuple_builder.h"
#include "binary_tuple_parser.h"

#include <ignite/common/bytes.h>

#include <stdexcept>
#include <string>

namespace ignite {

namespace {

void store_date(std::byte *dest, const ignite_date &value) {
    auto year = value.get_year();
    auto month = value.get_month();
    auto day = value.get_day_of_month();

    auto date = (year << 9) | (month << 5) | day;

    bytes::store<endian::LITTLE>(dest, std::uint16_t(date));
    bytes::store<endian::LITTLE>(dest + 2, std::uint8_t(date >> 16));
}

void store_time(std::byte *dest, const ignite_time &value, std::size_t size) {
    std::uint64_t hour = value.get_hour(); // NOLINT(cert-str34-c)
    std::uint64_t minute = value.get_minute(); // NOLINT(cert-str34-c)
    std::uint64_t second = value.get_second(); // NOLINT(cert-str34-c)
    std::uint64_t nanos = value.get_nano();

    if (size == 6) {
        auto time = (hour << 42) | (minute << 36) | (second << 30) | nanos;
        bytes::store<endian::LITTLE>(dest, std::uint32_t(time));
        bytes::store<endian::LITTLE>(dest + 4, std::uint16_t(time >> 32));
    } else if (size == 5) {
        auto time = (hour << 32) | (minute << 26) | (second << 20) | (nanos / 1000);
        bytes::store<endian::LITTLE>(dest, std::uint32_t(time));
        bytes::store<endian::LITTLE>(dest + 4, std::uint8_t(time >> 32));
    } else {
        auto time = (hour << 22) | (minute << 16) | (second << 10) | (nanos / 1000000);
        bytes::store<endian::LITTLE>(dest, std::uint32_t(time));
    }
}

} // namespace

binary_tuple_builder::binary_tuple_builder(tuple_num_t element_count) noexcept
    : element_count(element_count) {
}

void binary_tuple_builder::start() noexcept {
    element_index = 0;
    value_area_size = 0;
    entry_size = 0;
}

void binary_tuple_builder::layout() {
    using namespace ignite::binary_tuple_common;

    assert(element_index == element_count);

    binary_tuple_common::header header;

    entry_size = header.set_entry_size(value_area_size);

    std::size_t table_size = entry_size * element_count;

    binary_tuple.clear();
    binary_tuple.resize(HEADER_SIZE + table_size + value_area_size);

    binary_tuple[0] = header.flags;

    next_entry = binary_tuple.data() + HEADER_SIZE;
    value_base = next_entry + table_size;
    next_value = value_base;

    element_index = 0;
}

void binary_tuple_builder::append_varlen(bytes_view bytes) {
    if (bytes.empty() || bytes.front() == binary_tuple_common::VARLEN_EMPTY_BYTE) {
        *next_value++ = binary_tuple_common::VARLEN_EMPTY_BYTE;
    }

    append_bytes(bytes);
}

void binary_tuple_builder::append_bool(bool value) {
    static const std::byte true_byte{1};
    const tuple_size_t size = gauge_bool(value);
    append_bytes({&true_byte, size});
}

void binary_tuple_builder::append_int8(std::int8_t value) {
    const tuple_size_t size = gauge_int8(value);
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int8_ptr(std::int8_t *bytes) {
    const tuple_size_t size = gauge_int8(*bytes);
    append_bytes({reinterpret_cast<const std::byte *>(bytes), size});
}

void binary_tuple_builder::append_int16(std::int16_t value) {
    const tuple_size_t size = gauge_int16(value);

    if constexpr (!is_little_endian_platform()) {
        value = bytes::reverse(value);
    }
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int16_ptr(std::int16_t *bytes) {
    auto value = *bytes;

    const tuple_size_t size = gauge_int16(value);

    if constexpr (is_little_endian_platform()) {
        append_bytes({reinterpret_cast<const std::byte *>(bytes), size});
    } else {
        value = bytes::reverse(value);
        append_bytes({reinterpret_cast<const std::byte *>(&value), size});
    }
}

void binary_tuple_builder::append_int32(std::int32_t value) {
    const tuple_size_t size = gauge_int32(value);

    if constexpr (!is_little_endian_platform()) {
        value = bytes::reverse(value);
    }
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int32_ptr(std::int32_t *bytes) {
    auto value = *bytes;

    const tuple_size_t size = gauge_int32(value);

    if constexpr (is_little_endian_platform()) {
        append_bytes({reinterpret_cast<const std::byte *>(bytes), size});
    } else {
        value = bytes::reverse(value);
        append_bytes({reinterpret_cast<const std::byte *>(&value), size});
    }
}

void binary_tuple_builder::append_int64(std::int64_t value) {
    const tuple_size_t size = gauge_int64(value);

    if constexpr (!is_little_endian_platform()) {
        value = bytes::reverse(value);
    }
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int64_ptr(std::int64_t *bytes) {
    auto value = *bytes;

    const tuple_size_t size = gauge_int64(value);

    if constexpr (is_little_endian_platform()) {
        append_bytes({reinterpret_cast<const std::byte *>(bytes), size});
    } else {
        value = bytes::reverse(value);
        append_bytes({reinterpret_cast<const std::byte *>(&value), size});
    }
}

void binary_tuple_builder::append_float(float value) {
    const tuple_size_t size = gauge_float(value);
    assert(size == sizeof(float));
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    bytes::store<endian::LITTLE>(next_value, value);
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_double(double value) {
    const tuple_size_t size = gauge_double(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size == sizeof(float)) {
        const auto float_value = static_cast<float>(value);
        bytes::store<endian::LITTLE>(next_value, float_value);
    } else {
        assert(size == sizeof(double));
        bytes::store<endian::LITTLE>(next_value, value);
    }
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_number(const big_integer &value) {
    const tuple_size_t size = gauge_number(value);
    assert(size != 0);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    value.store_bytes(next_value);
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_number(const big_decimal &value) {
    const tuple_size_t size = gauge_number(value);
    assert(size != 0);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    std::int16_t scale = value.get_scale();
    if (value.is_zero())
        scale = 0;

    bytes::store<endian::LITTLE>(next_value, std::int16_t(scale));
    value.get_unscaled_value().store_bytes(next_value + 2);
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_uuid(uuid value) {
    const tuple_size_t size = gauge_uuid(value);
    assert(size == 16);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    bytes::store<endian::LITTLE>(next_value, value.get_most_significant_bits());
    bytes::store<endian::LITTLE>(next_value + 8, value.get_least_significant_bits());
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_date(const ignite_date &value) {
    const tuple_size_t size = gauge_date(value);
    assert(size == 3);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    store_date(next_value, value);
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_time(const ignite_time &value) {
    const tuple_size_t size = gauge_time(value);
    assert(4 <= size && size <= 6);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    store_time(next_value, value, size);
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_date_time(const ignite_date_time &value) {
    const tuple_size_t size = gauge_date_time(value);
    assert(7 <= size && size <= 9);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    store_date(next_value, value.date());
    store_time(next_value + 3, value.time(), size - 3);
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_timestamp(const ignite_timestamp &value) {
    const tuple_size_t size = gauge_timestamp(value);
    assert(size == 8 || size == 12);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    bytes::store<endian::LITTLE>(next_value, value.get_epoch_second());
    if (size == 12) {
        bytes::store<endian::LITTLE>(next_value + 8, value.get_nano());
    }
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_period(const ignite_period &value) {
    const tuple_size_t size = gauge_period(value);
    assert(size == 3 || size == 6 || size == 12);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size == 3) {
        bytes::store<endian::LITTLE>(next_value, std::uint8_t(value.get_years()));
        bytes::store<endian::LITTLE>(next_value + 1, std::uint8_t(value.get_months()));
        bytes::store<endian::LITTLE>(next_value + 2, std::uint8_t(value.get_days()));
    } else if (size == 6) {
        bytes::store<endian::LITTLE>(next_value, std::uint16_t(value.get_years()));
        bytes::store<endian::LITTLE>(next_value + 2, std::uint16_t(value.get_months()));
        bytes::store<endian::LITTLE>(next_value + 4, std::uint16_t(value.get_days()));
    } else {
        bytes::store<endian::LITTLE>(next_value, value.get_years());
        bytes::store<endian::LITTLE>(next_value + 4, value.get_months());
        bytes::store<endian::LITTLE>(next_value + 8, value.get_days());
    }
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_duration(const ignite_duration &value) {
    const tuple_size_t size = gauge_duration(value);
    assert(size == 8 || size == 12);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    bytes::store<endian::LITTLE>(next_value, value.get_seconds());
    if (size == 12) {
        bytes::store<endian::LITTLE>(next_value + 8, value.get_nano());
    }
    next_value += size;

    append_entry();
}

void binary_tuple_builder::append_bytes(bytes_view bytes) {
    assert(element_index < element_count);
    assert(next_value + bytes.size() <= value_base + value_area_size);

    std::memcpy(next_value, bytes.data(), bytes.size());
    next_value += bytes.size();

    append_entry();
}

} // namespace ignite
