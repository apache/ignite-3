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

#include "ignite/protocol/utils.h"
#include "ignite/protocol/reader.h"

#include "ignite/common/error_codes.h"

#include <msgpack.h>

#include <limits>
#include <mutex>
#include <random>
#include <sstream>
#include <type_traits>

namespace ignite::protocol {

/**
 * Error data extensions. When the server returns an error response, it may contain additional data in a map.
 * Keys are defined here.
 */
namespace error_extensions {
const std::string EXPECTED_SCHEMA_VERSION{"expected-schema-ver"};
}

/**
 * Check if int value fits in @c T.
 *
 * @tparam T Int type to fit value to.
 * @param value Int value.
 */
template<typename T>
inline void check_int_fits(std::int64_t value) {
    if (value > std::int64_t(std::numeric_limits<T>::max()))
        throw ignite_error("The number in stream is too large to fit in type: " + std::to_string(value));

    if (value < std::int64_t(std::numeric_limits<T>::min()))
        throw ignite_error("The number in stream is too small to fit in type: " + std::to_string(value));
}

template<typename T>
std::optional<T> try_unpack_int(const msgpack_object &object) {
    static_assert(
        std::numeric_limits<T>::is_integer && std::numeric_limits<T>::is_signed, "Type T is not a signed integer type");

    auto i64_val = try_unpack_object<std::int64_t>(object);
    if (!i64_val)
        return std::nullopt;

    check_int_fits<T>(*i64_val);
    return T(*i64_val);
}

template<>
std::optional<std::int64_t> try_unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_NEGATIVE_INTEGER && object.type != MSGPACK_OBJECT_POSITIVE_INTEGER)
        return std::nullopt;

    return object.via.i64;
}

template<>
std::optional<std::int32_t> try_unpack_object(const msgpack_object &object) {
    return try_unpack_int<std::int32_t>(object);
}

template<>
std::optional<std::string> try_unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_STR)
        return std::nullopt;

    return std::string{object.via.str.ptr, object.via.str.size};
}

template<typename T>
T unpack_int(const msgpack_object &object) {
    static_assert(
        std::numeric_limits<T>::is_integer && std::numeric_limits<T>::is_signed, "Type T is not a signed integer type");

    auto i64_val = unpack_object<std::int64_t>(object);

    check_int_fits<T>(i64_val);
    return T(i64_val);
}

template<typename T>
T unpack_uint(const msgpack_object &object) {
    static_assert(std::numeric_limits<T>::is_integer && !std::numeric_limits<T>::is_signed,
        "Type T is not a unsigned integer type");

    auto u64_val = unpack_object<std::uint64_t>(object);

    check_int_fits<T>(u64_val);
    return T(u64_val);
}

template<>
std::optional<std::string> unpack_nullable(const msgpack_object &object) {
    if (object.type == MSGPACK_OBJECT_NIL)
        return std::nullopt;

    return unpack_object<std::string>(object);
}

template<>
std::int64_t unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_NEGATIVE_INTEGER && object.type != MSGPACK_OBJECT_POSITIVE_INTEGER)
        throw ignite_error("The value in stream is not an integer number : " + std::to_string(object.type));

    return object.via.i64;
}

template<>
std::uint64_t unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_POSITIVE_INTEGER)
        throw ignite_error("The value in stream is not a positive integer number : " + std::to_string(object.type));

    return object.via.u64;
}

template<>
std::int32_t unpack_object(const msgpack_object &object) {
    return unpack_int<std::int32_t>(object);
}

template<>
std::int16_t unpack_object(const msgpack_object &object) {
    return unpack_int<std::int16_t>(object);
}

template<>
std::uint16_t unpack_object(const msgpack_object &object) {
    return unpack_uint<std::uint16_t>(object);
}

template<>
std::int8_t unpack_object(const msgpack_object &object) {
    return unpack_int<std::int8_t>(object);
}

template<>
std::uint8_t unpack_object(const msgpack_object &object) {
    return unpack_uint<std::uint8_t>(object);
}

template<>
std::string unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_STR)
        throw ignite_error("The value in stream is not a string : " + std::to_string(object.type));

    return {object.via.str.ptr, object.via.str.size};
}

template<>
uuid unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_EXT && object.via.ext.type != std::int8_t(extension_type::UUID))
        throw ignite_error("The value in stream is not a UUID : " + std::to_string(object.type));

    if (object.via.ext.size != 16)
        throw ignite_error("Unexpected UUID size: " + std::to_string(object.via.ext.size));

    auto data = reinterpret_cast<const std::byte *>(object.via.ext.ptr);

    auto msb = bytes::load<endian::LITTLE, int64_t>(data);
    auto lsb = bytes::load<endian::LITTLE, int64_t>(data + 8);

    return {msb, lsb};
}

template<>
bool unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_BOOLEAN)
        throw ignite_error("The value in stream is not a bool : " + std::to_string(object.type));

    return object.via.boolean;
}

bytes_view unpack_binary(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_BIN)
        throw ignite_error("The value in stream is not a Binary data : " + std::to_string(object.type));

    return {reinterpret_cast<const std::byte *>(object.via.bin.ptr), object.via.bin.size};
}

uuid make_random_uuid() {
    static std::mutex randomMutex;
    static std::random_device rd;
    static std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib;

    std::lock_guard<std::mutex> lock(randomMutex);

    return {distrib(gen), distrib(gen)};
}

std::optional<ignite_error> try_read_error(reader &reader) {
    if (reader.try_read_nil())
        return std::nullopt;

    return {read_error(reader)};
}

ignite_error read_error(reader &reader) {
    auto trace_id = reader.try_read_nil() ? make_random_uuid() : reader.read_uuid();
    auto code = reader.read_object_or_default<std::int32_t>(65537);
    auto class_name = reader.read_string();
    auto message = reader.read_string_nullable();
    auto java_stack_trace = reader.read_string_nullable();
    UNUSED_VALUE java_stack_trace;

    std::stringstream err_msg_builder;

    err_msg_builder << class_name;
    if (message)
        err_msg_builder << ": " << *message;
    err_msg_builder << " (" << code << ", " << trace_id << ")";

    std::optional<std::int32_t> ver{};
    if (!reader.try_read_nil()) {
        // Reading extensions
        auto num = reader.read_int32();
        for (std::int32_t i = 0; i < num; ++i) {
            auto key = reader.read_string();
            if (key == error_extensions::EXPECTED_SCHEMA_VERSION) {
                ver = reader.read_int32();
            } else {
                reader.skip();
            }
        }
    }

    return ignite_error{error::code(code), err_msg_builder.str(), ver};
}

void claim_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
    if (value.is_null()) {
        builder.claim_null(); // Type.
        builder.claim_null(); // Value.
        return;
    }

    switch (value.get_type()) {
        case ignite_type::BOOLEAN: {
            claim_type_and_scale(builder, ignite_type::BOOLEAN);
            builder.claim_bool(value.get<bool>());
            break;
        }
        case ignite_type::INT8: {
            claim_type_and_scale(builder, ignite_type::INT8);
            builder.claim_int8(value.get<std::int8_t>());
            break;
        }
        case ignite_type::INT16: {
            claim_type_and_scale(builder, ignite_type::INT16);
            builder.claim_int16(value.get<std::int16_t>());
            break;
        }
        case ignite_type::INT32: {
            claim_type_and_scale(builder, ignite_type::INT32);
            builder.claim_int32(value.get<std::int32_t>());
            break;
        }
        case ignite_type::INT64: {
            claim_type_and_scale(builder, ignite_type::INT64);
            builder.claim_int64(value.get<std::int64_t>());
            break;
        }
        case ignite_type::FLOAT: {
            claim_type_and_scale(builder, ignite_type::FLOAT);
            builder.claim_float(value.get<float>());
            break;
        }
        case ignite_type::DOUBLE: {
            claim_type_and_scale(builder, ignite_type::DOUBLE);
            builder.claim_double(value.get<double>());
            break;
        }
        case ignite_type::UUID: {
            claim_type_and_scale(builder, ignite_type::UUID);
            builder.claim_uuid(value.get<uuid>());
            break;
        }
        case ignite_type::STRING: {
            claim_type_and_scale(builder, ignite_type::STRING);
            builder.claim_varlen(value.get<std::string>());
            break;
        }
        case ignite_type::BYTE_ARRAY: {
            claim_type_and_scale(builder, ignite_type::BYTE_ARRAY);
            auto &data = value.get<std::vector<std::byte>>();
            builder.claim_varlen(data);
            break;
        }
        case ignite_type::DECIMAL: {
            const auto &dec_value = value.get<big_decimal>();
            claim_type_and_scale(builder, ignite_type::DECIMAL));
            builder.claim_number(dec_value);
            break;
        }
        case ignite_type::NUMBER: {
            claim_type_and_scale(builder, ignite_type::NUMBER);
            builder.claim_number(value.get<big_integer>());
            break;
        }
        case ignite_type::DATE: {
            claim_type_and_scale(builder, ignite_type::DATE);
            builder.claim_date(value.get<ignite_date>());
            break;
        }
        case ignite_type::TIME: {
            claim_type_and_scale(builder, ignite_type::TIME);
            builder.claim_time(value.get<ignite_time>());
            break;
        }
        case ignite_type::DATETIME: {
            claim_type_and_scale(builder, ignite_type::DATETIME);
            builder.claim_date_time(value.get<ignite_date_time>());
            break;
        }
        case ignite_type::TIMESTAMP: {
            claim_type_and_scale(builder, ignite_type::TIMESTAMP);
            builder.claim_timestamp(value.get<ignite_timestamp>());
            break;
        }
        case ignite_type::PERIOD: {
            claim_type_and_scale(builder, ignite_type::PERIOD);
            builder.claim_period(value.get<ignite_period>());
            break;
        }
        case ignite_type::DURATION: {
            claim_type_and_scale(builder, ignite_type::DURATION);
            builder.claim_duration(value.get<ignite_duration>());
            break;
        }
        case ignite_type::BITMASK: {
            claim_type_and_scale(builder, ignite_type::BITMASK);
            builder.claim_varlen(value.get<bit_array>().get_raw());
            break;
        }
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

void append_primitive_with_type(binary_tuple_builder &builder, const primitive &value) {
    if (value.is_null()) {
        builder.append_null(); // Type.
        builder.append_null(); // Value.
        return;
    }

    switch (value.get_type()) {
        case ignite_type::BOOLEAN: {
            append_type_and_scale(builder, ignite_type::BOOLEAN);
            builder.append_bool(value.get<bool>());
            break;
        }
        case ignite_type::INT8: {
            append_type_and_scale(builder, ignite_type::INT8);
            builder.append_int8(value.get<std::int8_t>());
            break;
        }
        case ignite_type::INT16: {
            append_type_and_scale(builder, ignite_type::INT16);
            builder.append_int16(value.get<std::int16_t>());
            break;
        }
        case ignite_type::INT32: {
            append_type_and_scale(builder, ignite_type::INT32);
            builder.append_int32(value.get<std::int32_t>());
            break;
        }
        case ignite_type::INT64: {
            append_type_and_scale(builder, ignite_type::INT64);
            builder.append_int64(value.get<std::int64_t>());
            break;
        }
        case ignite_type::FLOAT: {
            append_type_and_scale(builder, ignite_type::FLOAT);
            builder.append_float(value.get<float>());
            break;
        }
        case ignite_type::DOUBLE: {
            append_type_and_scale(builder, ignite_type::DOUBLE);
            builder.append_double(value.get<double>());
            break;
        }
        case ignite_type::UUID: {
            append_type_and_scale(builder, ignite_type::UUID);
            builder.append_uuid(value.get<uuid>());
            break;
        }
        case ignite_type::STRING: {
            append_type_and_scale(builder, ignite_type::STRING);
            builder.append_varlen(value.get<std::string>());
            break;
        }
        case ignite_type::BYTE_ARRAY: {
            append_type_and_scale(builder, ignite_type::BYTE_ARRAY);
            auto &data = value.get<std::vector<std::byte>>();
            builder.append_varlen(data);
            break;
        }
        case ignite_type::DECIMAL: {
            const auto &dec_value = value.get<big_decimal>();
            append_type_and_scale(builder, ignite_type::DECIMAL);
            builder.append_number(dec_value);
            break;
        }
        case ignite_type::NUMBER: {
            append_type_and_scale(builder, ignite_type::NUMBER);
            builder.append_number(value.get<big_integer>());
            break;
        }
        case ignite_type::DATE: {
            append_type_and_scale(builder, ignite_type::DATE);
            builder.append_date(value.get<ignite_date>());
            break;
        }
        case ignite_type::TIME: {
            append_type_and_scale(builder, ignite_type::TIME);
            builder.append_time(value.get<ignite_time>());
            break;
        }
        case ignite_type::DATETIME: {
            append_type_and_scale(builder, ignite_type::DATETIME);
            builder.append_date_time(value.get<ignite_date_time>());
            break;
        }
        case ignite_type::TIMESTAMP: {
            append_type_and_scale(builder, ignite_type::TIMESTAMP);
            builder.append_timestamp(value.get<ignite_timestamp>());
            break;
        }
        case ignite_type::PERIOD: {
            append_type_and_scale(builder, ignite_type::PERIOD);
            builder.append_period(value.get<ignite_period>());
            break;
        }
        case ignite_type::DURATION: {
            append_type_and_scale(builder, ignite_type::DURATION);
            builder.append_duration(value.get<ignite_duration>());
            break;
        }
        case ignite_type::BITMASK: {
            append_type_and_scale(builder, ignite_type::BITMASK);
            builder.append_varlen(value.get<bit_array>().get_raw());
            break;
        }
        default:
            throw ignite_error("Unsupported type: " + std::to_string(int(value.get_type())));
    }
}

primitive read_next_column(binary_tuple_parser &parser, ignite_type typ, std::int32_t scale) {
    auto val = parser.get_next();
    if (val.empty())
        return {};

    switch (typ) {
        case ignite_type::BOOLEAN:
            return binary_tuple_parser::get_bool(val);
        case ignite_type::INT8:
            return binary_tuple_parser::get_int8(val);
        case ignite_type::INT16:
            return binary_tuple_parser::get_int16(val);
        case ignite_type::INT32:
            return binary_tuple_parser::get_int32(val);
        case ignite_type::INT64:
            return binary_tuple_parser::get_int64(val);
        case ignite_type::FLOAT:
            return binary_tuple_parser::get_float(val);
        case ignite_type::DOUBLE:
            return binary_tuple_parser::get_double(val);
        case ignite_type::UUID:
            return binary_tuple_parser::get_uuid(val);
        case ignite_type::STRING:
            return std::string(binary_tuple_parser::get_varlen(val));
        case ignite_type::BYTE_ARRAY:
            return std::vector<std::byte>(binary_tuple_parser::get_varlen(val));
        case ignite_type::DECIMAL:
            return binary_tuple_parser::get_decimal(val);
        case ignite_type::NUMBER:
            return binary_tuple_parser::get_number(val);
        case ignite_type::DATE:
            return binary_tuple_parser::get_date(val);
        case ignite_type::TIME:
            return binary_tuple_parser::get_time(val);
        case ignite_type::DATETIME:
            return binary_tuple_parser::get_date_time(val);
        case ignite_type::TIMESTAMP:
            return binary_tuple_parser::get_timestamp(val);
        case ignite_type::PERIOD:
            return binary_tuple_parser::get_period(val);
        case ignite_type::DURATION:
            return binary_tuple_parser::get_duration(val);
        case ignite_type::BITMASK:
            return bit_array(binary_tuple_parser::get_varlen(val));
        default:
            throw ignite_error("Type with id " + std::to_string(int(typ)) + " is not yet supported");
    }
}

} // namespace ignite::protocol
