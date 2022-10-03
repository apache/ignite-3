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

#include "common/ignite_error.h"
#include "common/bytes.h"

#include "ignite/protocol/reader.h"
#include "ignite/protocol/utils.h"

namespace ignite::protocol {

Reader::Reader(bytes_view buffer)
    : m_buffer(buffer)
    , m_unpacker()
    , m_currentVal()
    , m_moveRes(MSGPACK_UNPACK_SUCCESS) {
    // TODO: Research if we can get rid of copying here.
    msgpack_unpacker_init(&m_unpacker, MSGPACK_UNPACKER_INIT_BUFFER_SIZE);
    msgpack_unpacker_reserve_buffer(&m_unpacker, m_buffer.size());
    memcpy(msgpack_unpacker_buffer(&m_unpacker), m_buffer.data(), m_buffer.size());
    msgpack_unpacker_buffer_consumed(&m_unpacker, m_buffer.size());

    msgpack_unpacked_init(&m_currentVal);

    next();
}

std::int64_t Reader::readInt64() {
    checkDataInStream();

    auto res = unpack_object<int64_t>(m_currentVal.data);
    next();

    return res;
}

std::string Reader::readString() {
    checkDataInStream();

    std::string res = unpack_object<std::string>(m_currentVal.data);
    next();

    return res;
}

std::optional<std::string> Reader::readStringNullable() {
    if (tryReadNil())
        return std::nullopt;

    return readString();
}

uuid Reader::readUuid() {
    checkDataInStream();

    uuid res = unpack_object<uuid>(m_currentVal.data);
    next();

    return res;
}

bool Reader::tryReadNil() {
    if (m_currentVal.data.type != MSGPACK_OBJECT_NIL)
        return false;

    next();
    return true;
}

void Reader::next() {
    checkDataInStream();

    m_moveRes = msgpack_unpacker_next(&m_unpacker, &m_currentVal);
}

} // namespace ignite::protocol
