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

#include "ignite/common/detail/bytes.h"
#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_error.h>
#include <ignite/common/primitive.h>
#include <ignite/common/uuid.h>
#include <ignite/protocol/extension_types.h>
#include <ignite/tuple/binary_tuple_builder.h>
#include <ignite/tuple/binary_tuple_parser.h>

#include <array>
#include <functional>
#include <optional>

#include <cstddef>
#include <cstdint>

struct msgpack_object;

namespace ignite::protocol {

class reader;

/**
 * Error data extensions. When the server returns an error response, it may contain additional data in a map.
 * Keys are defined here.
 */
struct error_extensions {
static const std::string EXPECTED_SCHEMA_VERSION;
static const std::string SQL_UPDATE_COUNTERS;
};

/** Magic bytes. */
static constexpr std::array<std::byte, 4> MAGIC_BYTES = {
    std::byte('I'), std::byte('G'), std::byte('N'), std::byte('I')};

template<typename T>
[[nodiscard]] std::optional<T> try_unpack_object(const msgpack_object &) {
    static_assert(sizeof(T) == 0, "Unpacking is not implemented for the type");
}

/**
 * Try unpack number.
 *
 * @param object MsgPack object.
 * @return Number or @c nullopt if the object is not a number.
 */
template<>
[[nodiscard]] std::optional<std::int64_t> try_unpack_object(const msgpack_object &object);

/**
 * Try unpack number.
 *
 * @param object MsgPack object.
 * @return Number or @c nullopt if the object is not a number.
 */
template<>
[[nodiscard]] std::optional<std::int32_t> try_unpack_object(const msgpack_object &object);

/**
 * Try unpack string.
 *
 * @param object MsgPack object.
 * @return String or @c nullopt if the object is not a string.
 */
template<>
[[nodiscard]] std::optional<std::string> try_unpack_object(const msgpack_object &object);

template<typename T>
[[nodiscard]] std::optional<T> unpack_nullable(const msgpack_object &) {
    static_assert(sizeof(T) == 0, "Unpacking is not implemented for the type");
}

/**
 * Unpack string.
 *
 * @param object MsgPack object.
 * @return String of @c nullopt if the object is @c nil.
 * @throw ignite_error if the object is not a string.
 */
template<>
[[nodiscard]] std::optional<std::string> unpack_nullable(const msgpack_object &object);

template<typename T>
[[nodiscard]] T unpack_object(const msgpack_object &) {
    static_assert(sizeof(T) == 0, "Unpacking is not implemented for the type");
}

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::int64_t unpack_object(const msgpack_object &object);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::uint64_t unpack_object(const msgpack_object &object);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::int32_t unpack_object(const msgpack_object &object);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::int16_t unpack_object(const msgpack_object &object);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::uint16_t unpack_object(const msgpack_object &object);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::int8_t unpack_object(const msgpack_object &object);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
[[nodiscard]] std::uint8_t unpack_object(const msgpack_object &object);

/**
 * Unpack string.
 *
 * @param object MsgPack object.
 * @return String.
 * @throw ignite_error if the object is not a string.
 */
template<>
[[nodiscard]] std::string unpack_object(const msgpack_object &object);

/**
 * Unpack UUID.
 *
 * @param object MsgPack object.
 * @return UUID.
 * @throw ignite_error if the object is not a UUID.
 */
template<>
[[nodiscard]] uuid unpack_object(const msgpack_object &object);

/**
 * Unpack bool.
 *
 * @param object MsgPack object.
 * @return bool value.
 * @throw ignite_error if the object is not a bool.
 */
template<>
[[nodiscard]] bool unpack_object(const msgpack_object &object);

/**
 * Get binary data.
 *
 * @param object Object.
 * @return Binary data view.
 */
[[nodiscard]] bytes_view unpack_binary(const msgpack_object &object);

/**
 * Make random UUID.
 *
 * @return Random UUID instance.
 */
[[nodiscard]] ignite::uuid make_random_uuid();

/**
 * Read error.
 *
 * @param reader reader.
 * @return Error if there is any.
 */
[[nodiscard]] std::optional<ignite_error> try_read_error(protocol::reader &reader);

/**
 * Read error core.
 *
 * @param reader reader.
 * @return Error.
 */
[[nodiscard]] ignite_error read_error(protocol::reader &reader);

/**
 * Claim type and scale header for a value written in binary tuple.
 *
 * @param builder Tuple builder.
 * @param typ Type.
 * @param scale Scale.
 */
inline void claim_type_and_scale(binary_tuple_builder &builder, ignite_type typ, std::int32_t scale = 0) {
    builder.claim_int32(static_cast<std::int32_t>(typ));
    builder.claim_int32(scale);
}

/**
 * Append type and scale header for a value written in binary tuple.
 *
 * @param builder Tuple builder.
 * @param typ Type.
 * @param scale Scale.
 */
inline void append_type_and_scale(binary_tuple_builder &builder, ignite_type typ, std::int32_t scale = 0) {
    builder.append_int32(static_cast<std::int32_t>(typ));
    builder.append_int32(scale);
}

/**
 * Claim space for a value with type header in tuple builder.
 *
 * @param builder Tuple builder.
 * @param value Value.
 */
void claim_primitive_with_type(binary_tuple_builder &builder, const primitive &value);

/**
 * Append a value with type header in tuple builder.
 *
 * @param builder Tuple builder.
 * @param value Value.
 */
void append_primitive_with_type(binary_tuple_builder &builder, const primitive &value);

/**
 * Read column value from binary tuple.
 *
 * @param parser Binary tuple parser.
 * @param typ Column type.
 * @param scale Column scale.
 * @return Column value.
 */
[[nodiscard]] primitive read_next_column(binary_tuple_parser &parser, ignite_type typ, std::int32_t scale);

} // namespace ignite::protocol
