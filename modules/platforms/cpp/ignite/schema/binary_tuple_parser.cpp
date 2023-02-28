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
T load_as(bytes_view bytes, std::size_t offset = 0) noexcept {
    return bytes::load<endian::LITTLE, T>(bytes.data() + offset);
}

ignite_date load_date(bytes_view bytes) {
    std::int32_t date = load_as<std::uint16_t>(bytes);
    date |= std::int32_t(load_as<std::int8_t>(bytes, 2)) << 16;

    std::int32_t day = date & 31;
    std::int32_t month = (date >> 5) & 15;
    std::int32_t year = (date >> 9); // Sign matters.

    return {year, month, day};
}

ignite_time load_time(bytes_view bytes) {
    std::uint64_t time = load_as<std::uint32_t>(bytes);

    std::int32_t nano;
    switch (bytes.size()) {
        case 4:
            nano = ((std::int32_t) time & ((1 << 10) - 1)) * 1000 * 1000;
            time >>= 10;
            break;
        case 5:
            time |= std::uint64_t(load_as<std::uint8_t>(bytes, 4)) << 32;
            nano = ((std::int32_t) time & ((1 << 20) - 1)) * 1000;
            time >>= 20;
            break;
        case 6:
            time |= std::uint64_t(load_as<std::uint16_t>(bytes, 4)) << 32;
            nano = ((std::int32_t) time & ((1 << 30) - 1));
            time >>= 30;
            break;
    }

    std::int_fast8_t second = ((int) time) & 63;
    std::int_fast8_t minute = ((int) time >> 6) & 63;
    std::int_fast8_t hour = ((int) time >> 12) & 31;

    return {hour, minute, second, nano};
}

} // namespace

binary_tuple_parser::binary_tuple_parser(IntT num_elements, bytes_view data)
    : binary_tuple_parser::binary_tuple_parser(num_elements, data, 0) {
    // No-op.
}

binary_tuple_parser::binary_tuple_parser(IntT num_elements, bytes_view data, IntT start_index)
    : binary_tuple(data)
    , element_count(num_elements)
    , element_index(start_index)
    , has_nullmap(false) {
    assert(!data.empty());

    binary_tuple_header header;
    header.flags = binary_tuple[0];

    entry_size = header.get_entry_size();
    has_nullmap = header.get_nullmap_flag();

    SizeT nullmap_size = 0;
    if (has_nullmap) {
        nullmap_size = binary_tuple_schema::get_nullmap_size(element_count);
    }

    SizeT table_size = entry_size * element_count;
    next_entry = binary_tuple.data() + binary_tuple_header::SIZE + nullmap_size;
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

element_view binary_tuple_parser::get_next() {
    assert(num_parsed_elements() < num_elements());

    ++element_index;

    // Load next entry offset (little-endian).
    std::uint64_t le_offset = 0;
    memcpy(&le_offset, next_entry, entry_size);
    next_entry += entry_size;

    const std::byte *value = next_value;
    next_value = value_base + bytes::ltoh(le_offset);

    const size_t length = next_value - value;
    if (length == 0 && has_nullmap && binary_tuple_schema::has_null(binary_tuple, element_index - 1)) {
        return {};
    }

    return bytes_view(value, length);
}

tuple_view binary_tuple_parser::parse(IntT num) {
    assert(element_index == 0);

    if (num == NO_NUM) {
        num = num_elements();
    }

    tuple_view tuple;
    tuple.reserve(num);
    while (element_index < num) {
        tuple.emplace_back(get_next());
    }

    return tuple;
}

key_tuple_view binary_tuple_parser::parse_key(IntT num) {
    assert(element_index == 0);

    if (num == NO_NUM) {
        num = num_elements();
    }

    key_tuple_view key;
    key.reserve(num);
    while (element_index < num) {
        key.emplace_back(get_next().value());
    }

    return key;
}

std::int8_t binary_tuple_parser::get_int8(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return load_as<std::int8_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int16_t binary_tuple_parser::get_int16(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return load_as<std::int8_t>(bytes);
        case 2:
            return load_as<std::int16_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int32_t binary_tuple_parser::get_int32(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return load_as<std::int8_t>(bytes);
        case 2:
            return load_as<std::int16_t>(bytes);
        case 4:
            return load_as<std::int32_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

std::int64_t binary_tuple_parser::get_int64(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0;
        case 1:
            return load_as<std::int8_t>(bytes);
        case 2:
            return load_as<std::int16_t>(bytes);
        case 4:
            return load_as<std::int32_t>(bytes);
        case 8:
            return load_as<std::int64_t>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

float binary_tuple_parser::get_float(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0.0f;
        case 4:
            return load_as<float>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

double binary_tuple_parser::get_double(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return 0.0f;
        case 4:
            return load_as<float>(bytes);
        case 8:
            return load_as<double>(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

big_integer binary_tuple_parser::get_number(bytes_view bytes) {
    return big_integer(bytes.data(), bytes.size());
}

big_decimal binary_tuple_parser::get_decimal(bytes_view bytes, int32_t scale) {
    big_integer mag(bytes.data(), bytes.size());
    return {std::move(mag), scale};
}

uuid binary_tuple_parser::get_uuid(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return uuid();
        case 16:
            return uuid(load_as<std::int64_t>(bytes), load_as<std::int64_t>(bytes, 8));
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_date binary_tuple_parser::get_date(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return ignite_date();
        case 3:
            return load_date(bytes);
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_time binary_tuple_parser::get_time(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return ignite_time();
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
        case 0:
            return ignite_date_time();
        case 7:
        case 8:
        case 9:
            return ignite_date_time(load_date(bytes), load_time(bytes.substr(3)));
        default:
            throw std::out_of_range("Bad element size");
    }
}

ignite_timestamp binary_tuple_parser::get_timestamp(bytes_view bytes) {
    switch (bytes.size()) {
        case 0:
            return ignite_timestamp();
        case 8: {
            std::int64_t seconds = load_as<std::int64_t>(bytes);
            return ignite_timestamp(seconds, 0);
        }
        case 12: {
            std::int64_t seconds = load_as<std::int64_t>(bytes);
            std::int32_t nanos = load_as<std::int32_t>(bytes, 8);
            return ignite_timestamp(seconds, nanos);
        }
        default:
            throw std::out_of_range("Bad element size");
    }
}

} // namespace ignite
