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
    std::uint64_t hour = value.get_hour();
    std::uint64_t minute = value.get_minute();
    std::uint64_t second = value.get_second();
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

binary_tuple_builder::binary_tuple_builder(IntT element_count) noexcept
    : element_count(element_count) {
}

void binary_tuple_builder::start() noexcept {
    element_index = 0;
    null_elements = 0;
    value_area_size = 0;
    entry_size = 0;
}

void binary_tuple_builder::layout() {
    assert(element_index == element_count);

    binary_tuple_header header;

    size_t nullmapSize = 0;
    if (null_elements) {
        header.set_nullmap_flag();
        nullmapSize = binary_tuple_schema::get_nullmap_size(element_count);
    }

    entry_size = header.set_entry_size(value_area_size);

    std::size_t tableSize = entry_size * element_count;

    binary_tuple.clear();
    binary_tuple.resize(binary_tuple_header::SIZE + nullmapSize + tableSize + value_area_size);

    binary_tuple[0] = header.flags;

    next_entry = binary_tuple.data() + binary_tuple_header::SIZE + nullmapSize;
    value_base = next_entry + tableSize;
    next_value = value_base;

    element_index = 0;
}

SizeT binary_tuple_builder::gauge(ignite_type type, bytes_view bytes) {
    switch (type) {
        case ignite_type::INT8:
            return gauge_int8(binary_tuple_parser::get_int8(bytes));
        case ignite_type::INT16:
            return gauge_int16(binary_tuple_parser::get_int16(bytes));
        case ignite_type::INT32:
            return gauge_int32(binary_tuple_parser::get_int32(bytes));
        case ignite_type::INT64:
            return gauge_int64(binary_tuple_parser::get_int64(bytes));
        case ignite_type::FLOAT:
            return gauge_float(binary_tuple_parser::get_float(bytes));
        case ignite_type::DOUBLE:
            return gauge_double(binary_tuple_parser::get_double(bytes));
        case ignite_type::NUMBER:
        case ignite_type::DECIMAL:
            // For a decimal there is no point to know scale here, treating it as big_integer will do.
            return gauge_number(binary_tuple_parser::get_number(bytes));
        case ignite_type::STRING:
        case ignite_type::BINARY:
        case ignite_type::BITMASK:
            return static_cast<SizeT>(bytes.size());
        case ignite_type::UUID:
            return gauge_uuid(binary_tuple_parser::get_uuid(bytes));
        case ignite_type::DATE:
            return gauge_date(binary_tuple_parser::get_date(bytes));
        case ignite_type::TIME:
            return gauge_time(binary_tuple_parser::get_time(bytes));
        case ignite_type::DATETIME:
            return gauge_date_time(binary_tuple_parser::get_date_time(bytes));
        case ignite_type::TIMESTAMP:
            return gauge_timestamp(binary_tuple_parser::get_timestamp(bytes));
        default:
            throw std::logic_error("Unsupported type " + std::to_string(static_cast<int>(type)));
    }
}

void binary_tuple_builder::append_bytes(bytes_view bytes) {
    assert(element_index < element_count);
    assert(next_value + bytes.size() <= value_base + value_area_size);
    std::memcpy(next_value, bytes.data(), bytes.size());
    next_value += bytes.size();
    append_entry();
}

void binary_tuple_builder::append_int8(bytes_view bytes) {
    SizeT size = gauge_int8(binary_tuple_parser::get_int8(bytes));
    assert(size <= bytes.size());
    append_bytes({bytes.data(), size});
}

void binary_tuple_builder::append_int8(std::int8_t value) {
    SizeT size = gauge_int8(value);
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int16(bytes_view bytes) {
    auto value = binary_tuple_parser::get_int16(bytes);

    SizeT size = gauge_int16(value);
    assert(size <= bytes.size());

    if constexpr (is_little_endian_platform()) {
        append_bytes({bytes.data(), size});
    } else {
        value = bytes::reverse(value);
        append_bytes({reinterpret_cast<const std::byte *>(&value), size});
    }
}

void binary_tuple_builder::append_int16(std::int16_t value) {
    SizeT size = gauge_int16(value);

    if constexpr (!is_little_endian_platform()) {
        value = bytes::reverse(value);
    }
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int32(bytes_view bytes) {
    auto value = binary_tuple_parser::get_int32(bytes);

    SizeT size = gauge_int32(value);
    assert(size <= bytes.size());

    if constexpr (is_little_endian_platform()) {
        append_bytes({bytes.data(), size});
    } else {
        value = bytes::reverse(value);
        append_bytes({reinterpret_cast<const std::byte *>(&value), size});
    }
}

void binary_tuple_builder::append_int32(std::int32_t value) {
    SizeT size = gauge_int32(value);

    if constexpr (!is_little_endian_platform()) {
        value = bytes::reverse(value);
    }
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_int64(bytes_view bytes) {
    auto value = binary_tuple_parser::get_int64(bytes);

    SizeT size = gauge_int64(value);
    assert(size <= bytes.size());

    if constexpr (is_little_endian_platform()) {
        append_bytes({bytes.data(), size});
    } else {
        value = bytes::reverse(value);
        append_bytes({reinterpret_cast<const std::byte *>(&value), size});
    }
}

void binary_tuple_builder::append_int64(std::int64_t value) {
    SizeT size = gauge_int64(value);

    if constexpr (!is_little_endian_platform()) {
        value = bytes::reverse(value);
    }
    append_bytes({reinterpret_cast<const std::byte *>(&value), size});
}

void binary_tuple_builder::append_float(bytes_view bytes) {
    float value = binary_tuple_parser::get_float(bytes);
    append_float(value);
}

void binary_tuple_builder::append_float(float value) {
    SizeT size = gauge_float(value);

    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        assert(size == sizeof(float));
        bytes::store<endian::LITTLE>(next_value, value);
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_double(bytes_view bytes) {
    double value = binary_tuple_parser::get_double(bytes);
    append_double(value);
}

void binary_tuple_builder::append_double(double value) {
    SizeT size = gauge_double(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        if (size == sizeof(float)) {
            float floatValue = static_cast<float>(value);
            bytes::store<endian::LITTLE>(next_value, floatValue);
        } else {
            assert(size == sizeof(double));
            bytes::store<endian::LITTLE>(next_value, value);
        }
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_number(bytes_view bytes) {
    big_integer value = binary_tuple_parser::get_number(bytes);
    append_number(value);
}

void binary_tuple_builder::append_number(const big_integer &value) {
    SizeT size = gauge_number(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        value.store_bytes(next_value);
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_number(const big_decimal &value) {
    SizeT size = gauge_number(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        value.get_unscaled_value().store_bytes(next_value);
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_uuid(bytes_view bytes) {
    auto value = binary_tuple_parser::get_uuid(bytes);
    append_uuid(value);
}

void binary_tuple_builder::append_uuid(uuid value) {
    SizeT size = gauge_uuid(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        assert(size == 16);
        bytes::store<endian::LITTLE>(next_value, value.getMostSignificantBits());
        bytes::store<endian::LITTLE>(next_value + 8, value.getLeastSignificantBits());
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_date(bytes_view bytes) {
    auto value = binary_tuple_parser::get_date(bytes);
    append_date(value);
}

void binary_tuple_builder::append_date(const ignite_date &value) {
    SizeT size = gauge_date(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        assert(size == 3);
        store_date(next_value, value);
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_time(bytes_view bytes) {
    auto value = binary_tuple_parser::get_time(bytes);
    append_time(value);
}

void binary_tuple_builder::append_time(const ignite_time &value) {
    SizeT size = gauge_time(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        assert(4 <= size && size <= 6);
        store_time(next_value, value, size);
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_date_time(bytes_view bytes) {
    auto value = binary_tuple_parser::get_date_time(bytes);
    append_date_time(value);
}

void binary_tuple_builder::append_date_time(const ignite_date_time &value) {
    SizeT size = gauge_date_time(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        assert(7 <= size && size <= 9);
        store_date(next_value, value.date());
        store_time(next_value + 3, value.time(), size - 3);
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append_timestamp(bytes_view bytes) {
    auto value = binary_tuple_parser::get_timestamp(bytes);
    append_timestamp(value);
}

void binary_tuple_builder::append_timestamp(const ignite_timestamp &value) {
    SizeT size = gauge_timestamp(value);
    assert(element_index < element_count);
    assert(next_value + size <= value_base + value_area_size);

    if (size != 0) {
        assert(size == 8 || size == 12);
        bytes::store<endian::LITTLE>(next_value, value.get_epoch_second());
        if (size == 12) {
            bytes::store<endian::LITTLE>(next_value + 8, value.get_nano());
        }
        next_value += size;
    }

    append_entry();
}

void binary_tuple_builder::append(ignite_type type, const bytes_view &bytes) {
    switch (type) {
        case ignite_type::INT8:
            return append_int8(bytes);
        case ignite_type::INT16:
            return append_int16(bytes);
        case ignite_type::INT32:
            return append_int32(bytes);
        case ignite_type::INT64:
            return append_int64(bytes);
        case ignite_type::FLOAT:
            return append_float(bytes);
        case ignite_type::DOUBLE:
            return append_double(bytes);
        case ignite_type::NUMBER:
        case ignite_type::DECIMAL:
            // For a decimal there is no point to know scale here, treating it as big_integer will do.
            return append_number(bytes);
        case ignite_type::STRING:
        case ignite_type::BINARY:
        case ignite_type::BITMASK:
            return append_bytes(bytes);
        case ignite_type::UUID:
            return append_uuid(bytes);
        case ignite_type::DATE:
            return append_date(bytes);
        case ignite_type::TIME:
            return append_time(bytes);
        case ignite_type::DATETIME:
            return append_date_time(bytes);
        case ignite_type::TIMESTAMP:
            return append_timestamp(bytes);
        default:
            throw std::logic_error("Unsupported type " + std::to_string(static_cast<int>(type)));
    }
}

} // namespace ignite
