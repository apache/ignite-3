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

#include "binary_tuple_parser.h"

#include <ignite/common/bytes.h>

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace ignite {

namespace {

template<typename T>
T load_little(bytes_view view, std::size_t offset = 0) noexcept {
    return bytes::load<endian::LITTLE, T>(view.data() + offset);
}

ignite_date load_date(bytes_view bytes) {
    std::int32_t date = load_little<std::uint16_t>(bytes);
    date |= std::int32_t(load_little<std::int8_t>(bytes, 2)) << 16;

    std::int32_t day = date & 31;
    std::int32_t month = (date >> 5) & 15;
    std::int32_t year = (date >> 9); // Sign matters.

    return {year, month, day};
}

ignite_time load_time(bytes_view bytes) {
    std::uint64_t time = load_little<std::uint32_t>(bytes);

    std::int32_t nano = 0;
    switch (bytes.size()) {
        case 4:
            nano = ((std::int32_t) time & ((1 << 10) - 1)) * 1000 * 1000;
            time >>= 10;
            break;
        case 5:
            time |= std::uint64_t(load_little<std::uint8_t>(bytes, 4)) << 32;
            nano = ((std::int32_t) time & ((1 << 20) - 1)) * 1000;
            time >>= 20;
            break;
        case 6:
            time |= std::uint64_t(load_little<std::uint16_t>(bytes, 4)) << 32;
            nano = ((std::int32_t) time & ((1 << 30) - 1));
            time >>= 30;
            break;
    }

    auto second = std::int_fast8_t(time & 63);
    auto minute = std::int_fast8_t((time >> 6) & 63);
    auto hour = std::int_fast8_t((time >> 12) & 31);

    return {hour, minute, second, nano};
}

} // namespace

binary_tuple_parser::binary_tuple_parser(tuple_num_t num_elements, bytes_view data)
    : binary_tuple(data)
    , element_count(num_elements)
    , element_index(0) {
    assert(!data.empty());

    binary_tuple_common::header header;
    header.flags = binary_tuple[0];

    entry_size = header.get_entry_size();

    tuple_size_t table_size = entry_size * element_count;
    next_entry = binary_tuple.data() + binary_tuple_common::HEADER_SIZE;
    value_base = next_entry + table_size;

    if (value_base > binary_tuple.data() + binary_tuple.size()) {
        throw std::out_of_range("Too short byte buffer");
    }

    next_value = value_base;

    // Load the tuple end offset (little-endian).
    std::uint64_t le_end_offset = 0;
    memcpy(&le_end_offset, value_base - entry_size, entry_size);

    // Fix tuple size if needed.
    const std::byte *tuple_end = value_base + bytes::ltoh(le_end_offset);
    const std::byte *given_end = binary_tuple.data() + binary_tuple.size();
    if (given_end > tuple_end) {
        binary_tuple.remove_suffix(given_end - tuple_end);
    } else if (given_end < tuple_end) {
        throw std::out_of_range("Too short byte buffer");
    }
}

bytes_view binary_tuple_parser::get_next() {
    using namespace ignite::binary_tuple_common;

    assert(num_parsed_elements() < num_elements());

    // Load next entry offset (little-endian).
    std::uint64_t le_offset = 0;
    memcpy(&le_offset, next_entry, entry_size);
    next_entry += entry_size;

    const std::byte *value = next_value;
    next_value = value_base + bytes::ltoh(le_offset);

    element_index++;

    if (std::size_t length = next_value - value) {
        return {value, length};
    }

    return {};
}

bytes_view binary_tuple_parser::get_varlen(bytes_view bytes) {
    switch (bytes.size()) {
        default:
            if (bytes.front() == binary_tuple_common::VARLEN_EMPTY_BYTE) {
                bytes.remove_prefix(1);
            }
            return bytes;
        case 0:
            throw std::out_of_range("Bad element size");
    }
}

bool binary_tuple_parser::get_bool(bytes_view bytes) {
    switch (bytes.size()) {
        case 1:
            return load_little<std::int8_t>(bytes) != 0;
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int8_t binary_tuple_parser::get_int8(bytes_view bytes) {
    switch (bytes.size()) {
        case 1:
            return load_little<std::int8_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int16_t binary_tuple_parser::get_int16(bytes_view bytes) {
    switch (bytes.size()) {
        case 1:
            return load_little<std::int8_t>(bytes);
        case 2:
            return load_little<std::int16_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int32_t binary_tuple_parser::get_int32(bytes_view bytes) {
    switch (bytes.size()) {
        case 1:
            return load_little<std::int8_t>(bytes);
        case 2:
            return load_little<std::int16_t>(bytes);
        case 4:
            return load_little<std::int32_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int64_t binary_tuple_parser::get_int64(bytes_view bytes) {
    switch (bytes.size()) {
        case 1:
            return load_little<std::int8_t>(bytes);
        case 2:
            return load_little<std::int16_t>(bytes);
        case 4:
            return load_little<std::int32_t>(bytes);
        case 8:
            return load_little<std::int64_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

float binary_tuple_parser::get_float(bytes_view bytes) {
    switch (bytes.size()) {
        case 4:
            return load_little<float>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

double binary_tuple_parser::get_double(bytes_view bytes) {
    switch (bytes.size()) {
        case 4:
            return load_little<float>(bytes);
        case 8:
            return load_little<double>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

big_integer binary_tuple_parser::get_number(bytes_view bytes) {
    return {bytes.data(), bytes.size()};
}

big_decimal binary_tuple_parser::get_decimal(bytes_view bytes) {
    auto val_scale = load_little<std::int16_t>(bytes);

    big_integer mag(bytes.data() + 2, bytes.size() - 2);
    return {std::move(mag), val_scale};
}

big_decimal binary_tuple_parser::get_decimal(bytes_view bytes, std::int16_t scale) {
    auto val = get_decimal(bytes);

    if (val.get_scale() > scale)
        throw std::out_of_range("Invalid scale, expected less than or equal to " + std::to_string(scale) + ", but was "
            + std::to_string(val.get_scale()));

    val.set_scale(scale, val);
    return val;
}

uuid binary_tuple_parser::get_uuid(bytes_view bytes) {
    switch (bytes.size()) {
        case 16:
            return {load_little<std::int64_t>(bytes), load_little<std::int64_t>(bytes, 8)};
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_date binary_tuple_parser::get_date(bytes_view bytes) {
    switch (bytes.size()) {
        case 3:
            return load_date(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_time binary_tuple_parser::get_time(bytes_view bytes) {
    switch (bytes.size()) {
        case 4:
        case 5:
        case 6:
            return load_time(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_date_time binary_tuple_parser::get_date_time(bytes_view bytes) {
    switch (bytes.size()) {
        case 7:
        case 8:
        case 9:
            return {load_date(bytes), load_time(bytes.substr(3))};
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_timestamp binary_tuple_parser::get_timestamp(bytes_view bytes) {
    switch (bytes.size()) {
        case 8: {
            auto seconds = load_little<std::int64_t>(bytes);
            return {seconds, 0};
        }
        case 12: {
            auto seconds = load_little<std::int64_t>(bytes);
            auto nanos = load_little<std::int32_t>(bytes, 8);
            return {seconds, nanos};
        }
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_period binary_tuple_parser::get_period(bytes_view bytes) {
    switch (bytes.size()) {
        case 3: {
            auto years = load_little<std::int8_t>(bytes);
            auto months = load_little<std::int8_t>(bytes, 1);
            auto days = load_little<std::int8_t>(bytes, 2);
            return {years, months, days};
        }
        case 6: {
            auto years = load_little<std::int16_t>(bytes);
            auto months = load_little<std::int16_t>(bytes, 2);
            auto days = load_little<std::int16_t>(bytes, 4);
            return {years, months, days};
        }
        case 12: {
            auto years = load_little<std::int32_t>(bytes);
            auto months = load_little<std::int32_t>(bytes, 4);
            auto days = load_little<std::int32_t>(bytes, 8);
            return {years, months, days};
        }
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_duration binary_tuple_parser::get_duration(bytes_view bytes) {
    switch (bytes.size()) {
        case 8: {
            auto seconds = load_little<std::int64_t>(bytes);
            return {seconds, 0};
        }
        case 12: {
            auto seconds = load_little<std::int64_t>(bytes);
            auto nanos = load_little<std::int32_t>(bytes, 8);
            return {seconds, nanos};
        }
        default:
            throw std::out_of_range("Bad element size");
    }
}

} // namespace ignite
