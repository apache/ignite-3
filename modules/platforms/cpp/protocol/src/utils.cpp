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

#include <mutex>
#include <random>
#include <sstream>

#include <msgpack.h>

#include "ignite/protocol/reader.h"
#include "ignite/protocol/utils.h"

namespace ignite::protocol {

template<>
int64_t unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_NEGATIVE_INTEGER && object.type != MSGPACK_OBJECT_POSITIVE_INTEGER)
        throw ignite_error("The value in stream is not an integer number");

    return object.via.i64;
}

template<>
uuid unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_EXT && object.via.ext.type != std::int8_t(extension_type::UUID))
        throw ignite_error("The value in stream is not a UUID");

    if (object.via.ext.size != 16)
        throw ignite_error("Unexpected UUID size: " + std::to_string(object.via.ext.size));

    auto data = reinterpret_cast<const std::byte *>(object.via.ext.ptr);

    auto msb = bytes::load<Endian::LITTLE, int64_t>(data);
    auto lsb = bytes::load<Endian::LITTLE, int64_t>(data + 8);

    return {msb, lsb};
}

template<>
std::string unpack_object(const msgpack_object &object) {
    if (object.type != MSGPACK_OBJECT_STR)
        throw ignite_error("The value in stream is not a string");

    return {object.via.str.ptr, object.via.str.size};
}

uuid makeRandomUuid() {
    static std::mutex randomMutex;
    static std::random_device rd;
    static std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib;

    std::lock_guard<std::mutex> lock(randomMutex);

    return {distrib(gen), distrib(gen)};
}

std::optional<ignite_error> readError(Reader &reader) {
    if (reader.tryReadNil())
        return std::nullopt;

    uuid traceId = reader.tryReadNil() ? makeRandomUuid() : reader.readUuid();
    int32_t code = reader.tryReadNil() ? 65537 : reader.readInt32();
    std::string className = reader.readString();
    std::string message = reader.readString();

    std::stringstream errMsgBuilder;

    errMsgBuilder << className << ": " << message << " (" << code << ", " << traceId << ")";

    return {ignite_error(status_code(code), errMsgBuilder.str())};
}

} // namespace ignite::protocol
