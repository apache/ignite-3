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

#include <ignite/common/bytes.h>
#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_error.h>
#include <ignite/common/uuid.h>
#include <ignite/protocol/extension_types.h>

#include <array>
#include <functional>
#include <optional>

#include <cstddef>
#include <cstdint>

struct msgpack_object;

namespace ignite::protocol {

class reader;

/** Magic bytes. */
static constexpr std::array<std::byte, 4> MAGIC_BYTES = {
    std::byte('I'), std::byte('G'), std::byte('N'), std::byte('I')};

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
 * Get array size.
 *
 * @param object Object.
 * @return Array size.
 */
[[nodiscard]] std::uint32_t unpack_array_size(const msgpack_object &object);

/**
 * Unpack array.
 *
 * @param object Object.
 */
void unpack_array_raw(const msgpack_object &object, const std::function<void(const msgpack_object &)> &read_func);

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
[[nodiscard]] std::optional<ignite_error> read_error(protocol::reader &reader);

} // namespace ignite::protocol
