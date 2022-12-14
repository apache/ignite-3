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

#include <msgpack.h>

#include <limits>
#include <mutex>
#include <random>
#include <sstream>
#include <type_traits>

namespace ignite::protocol {

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
std::int32_t unpack_object(const msgpack_object &object) {
    return unpack_int<std::int32_t>(object);
}

template<>
std::int16_t unpack_object(const msgpack_object &object) {
    return unpack_int<std::int16_t>(object);
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

std::uint32_t unpack_array_size(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_ARRAY)
        throw ignite_error("The value in stream is not an Array : " + std::to_string(object.type));
    return object.via.array.size;
}

void unpack_array_raw(const msgpack_object &object, const std::function<void(const msgpack_object &)> &read_func) {
    auto size = unpack_array_size(object);
    for (std::uint32_t i = 0; i < size; ++i)
        read_func(object.via.array.ptr[i]);
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

std::optional<ignite_error> read_error(reader &reader) {
    if (reader.try_read_nil())
        return std::nullopt;

    auto trace_id = reader.try_read_nil() ? make_random_uuid() : reader.read_uuid();
    auto code = reader.read_object_or_default<std::int32_t>(65537);
    auto class_name = reader.read_string();
    auto message = reader.read_string();

    std::stringstream errMsgBuilder;

    errMsgBuilder << class_name << ": " << message << " (" << code << ", " << trace_id << ")";

    return {ignite_error(status_code(code), errMsgBuilder.str())};
}

} // namespace ignite::protocol
