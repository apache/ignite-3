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

#include "../common/bytes.h"

#include <stdexcept>
#include <string>

namespace ignite {

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
        case ignite_type::STRING:
        case ignite_type::BINARY:
            return static_cast<SizeT>(bytes.size());

        case ignite_type::UUID:
        case ignite_type::DATE:
        case ignite_type::TIME:
        case ignite_type::DATETIME:
        case ignite_type::TIMESTAMP:
        case ignite_type::BITMASK:
        case ignite_type::NUMBER:
        case ignite_type::DECIMAL:
            // TODO: support the types above with IGNITE-17401.
        default:
            throw std::logic_error("Unsupported type " + std::to_string(static_cast<int>(type)));
    }
}

void binary_tuple_builder::put_bytes(bytes_view bytes) {
    assert(element_index < element_count);
    assert(next_value + bytes.size() <= value_base + value_area_size);
    std::memcpy(next_value, bytes.data(), bytes.size());
    next_value += bytes.size();
    append_entry();
}

void binary_tuple_builder::put_int8(bytes_view bytes) {
    SizeT size = gauge_int8(binary_tuple_parser::get_int8(bytes));
    assert(size <= bytes.size());
    put_bytes(bytes_view{bytes.data(), size});
}

void binary_tuple_builder::put_int16(bytes_view bytes) {
    SizeT size = gauge_int16(binary_tuple_parser::get_int16(bytes));
    assert(size <= bytes.size());
    static_assert(is_little_endian_platform());
    put_bytes(bytes_view{bytes.data(), size});
}

void binary_tuple_builder::put_int32(bytes_view bytes) {
    SizeT size = gauge_int32(binary_tuple_parser::get_int32(bytes));
    assert(size <= bytes.size());
    static_assert(is_little_endian_platform());
    put_bytes(bytes_view{bytes.data(), size});
}

void binary_tuple_builder::put_int64(bytes_view bytes) {
    SizeT size = gauge_int64(binary_tuple_parser::get_int64(bytes));
    assert(size <= bytes.size());
    static_assert(is_little_endian_platform());
    put_bytes(bytes_view{bytes.data(), size});
}

void binary_tuple_builder::put_float(bytes_view bytes) {
    SizeT size = gauge_float(binary_tuple_parser::get_float(bytes));
    assert(size <= bytes.size());
    put_bytes(bytes_view{bytes.data(), size});
}

void binary_tuple_builder::put_double(bytes_view bytes) {
    double value = binary_tuple_parser::get_double(bytes);
    SizeT size = gauge_double(value);
    assert(size <= bytes.size());
    if (size != sizeof(float)) {
        put_bytes(bytes_view{bytes.data(), size});
    } else {
        float floatValue = static_cast<float>(value);
        put_bytes(bytes_view{reinterpret_cast<std::byte *>(&floatValue), sizeof(float)});
    }
}

void binary_tuple_builder::append(ignite_type type, const bytes_view &bytes) {
    switch (type) {
        case ignite_type::INT8:
            return put_int8(bytes);
        case ignite_type::INT16:
            return put_int16(bytes);
        case ignite_type::INT32:
            return put_int32(bytes);
        case ignite_type::INT64:
            return put_int64(bytes);
        case ignite_type::FLOAT:
            return put_float(bytes);
        case ignite_type::DOUBLE:
            return put_double(bytes);
        case ignite_type::STRING:
        case ignite_type::BINARY:
            return put_bytes(bytes);

        case ignite_type::UUID:
        case ignite_type::DATE:
        case ignite_type::TIME:
        case ignite_type::DATETIME:
        case ignite_type::TIMESTAMP:
        case ignite_type::BITMASK:
        case ignite_type::NUMBER:
        case ignite_type::DECIMAL:
            // TODO: support the types above with IGNITE-17401.
        default:
            throw std::logic_error("Unsupported type " + std::to_string(static_cast<int>(type)));
    }
}

} // namespace ignite
