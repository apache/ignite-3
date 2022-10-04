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

#include <cstdint>
#include <functional>

#include <msgpack.h>

#include "common/uuid.h"
#include "common/types.h"

#include "ignite/protocol/utils.h"

namespace ignite::protocol {

/**
 * Reader.
 */
class Reader {
public:
    // Deleted
    Reader() = delete;
    Reader(Reader &&) = delete;
    Reader(const Reader &) = delete;
    Reader &operator=(Reader &&) = delete;
    Reader &operator=(const Reader &) = delete;

    /**
     * Constructor.
     *
     * @param buffer Buffer.
     */
    explicit Reader(bytes_view buffer);

    /**
     * Destructor.
     */
    ~Reader() { msgpack_unpacker_destroy(&m_unpacker); }

    /**
     * Read int16.
     *
     * @return Value.
     */
    [[nodiscard]] std::int16_t readInt16() { return std::int16_t(readInt64()); }

    /**
     * Read int32.
     *
     * @return Value.
     */
    [[nodiscard]] std::int32_t readInt32() { return std::int32_t(readInt64()); }

    /**
     * Read int64 number.
     *
     * @return Value.
     */
    [[nodiscard]] std::int64_t readInt64();

    /**
     * Read string.
     *
     * @return String value.
     */
    [[nodiscard]] std::string readString();

    /**
     * Read string.
     *
     * @return String value or nullopt.
     */
    [[nodiscard]] std::optional<std::string> readStringNullable();

    /**
     * Read UUID.
     *
     * @return UUID value.
     */
    [[nodiscard]] uuid readUuid();

    /**
     * Read Map size.
     *
     * @return Map size.
     */
    [[nodiscard]] uint32_t readMapSize() const {
        checkDataInStream();

        if (m_currentVal.data.type != MSGPACK_OBJECT_MAP)
            throw ignite_error("The value in stream is not a Map");

        return m_currentVal.data.via.map.size;
    }

    /**
     * Read Map.
     *
     * @tparam K Key type.
     * @tparam V Value type.
     * @param handler Pair handler.
     */
     template<typename K, typename V>
     void readMap(const std::function<void(K&&, V&&)>& handler) {
        auto size = readMapSize();
        for (std::uint32_t i = 0; i < size; ++i) {
            auto key = unpack_object<K>(m_currentVal.data.via.map.ptr[i].key);
            auto val = unpack_object<V>(m_currentVal.data.via.map.ptr[i].val);
            handler(std::move(key), std::move(val));
        }
    }

    /**
     * If the next value is Nil, read it and move reader to the next position.
     *
     * @return @c true if the value was nil.
     */
    bool tryReadNil();

    /**
     * Skip next value.
     */
    void skip() { next(); }

private:
    /**
     * Move to the next value.
     */
    void next();

    /**
     * Check whether there is a data in stream and throw ignite_error if there is none.
     */
    void checkDataInStream() const {
        if (m_moveRes < 0)
            throw ignite_error("No more data in stream");
    }

    /** Buffer. */
    bytes_view m_buffer;

    /** Unpacker. */
    msgpack_unpacker m_unpacker;

    /** Current value. */
    msgpack_unpacked m_currentVal;

    /** Result of the last move operation. */
    msgpack_unpack_return m_moveRes;
};

} // namespace ignite::protocol
