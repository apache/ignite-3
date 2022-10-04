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

#include <cstddef>
#include <cstdint>

#include <array>
#include <optional>

#include "common/ignite_error.h"
#include "common/bytes.h"
#include "common/types.h"
#include "common/uuid.h"

#include "ignite/protocol/extension_types.h"

struct msgpack_object;

namespace ignite::protocol {

class reader;

/** Magic bytes. */
static constexpr std::array<std::byte, 4> MAGIC_BYTES = {
    std::byte('I'), std::byte('G'), std::byte('N'), std::byte('I')};

template<typename T>
T unpack_object(const msgpack_object&);

/**
 * Unpack number.
 *
 * @param object MsgPack object.
 * @return Number.
 * @throw ignite_error if the object is not a number.
 */
template<>
int64_t unpack_object(const msgpack_object& object);

/**
 * Unpack string.
 *
 * @param object MsgPack object.
 * @return String.
 * @throw ignite_error if the object is not a string.
 */
template<>
std::string unpack_object(const msgpack_object& object);

/**
 * Unpack UUID.
 *
 * @param object MsgPack object.
 * @return UUID.
 * @throw ignite_error if the object is not a UUID.
 */
template<>
uuid unpack_object(const msgpack_object& object);

/**
 * Make random UUID.
 *
 * @return Random UUID instance.
 */
ignite::uuid makeRandomUuid();

/**
 * Read error.
 *
 * @param reader reader.
 * @return Error.
 */
std::optional<ignite_error> readError(protocol::reader &reader);

} // namespace ignite::protocol
