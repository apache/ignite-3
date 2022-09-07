/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

#include "common/guid.h"
#include "common/Types.h"

namespace ignite::protocol
{

/**
 * Reader.
 */
class Reader
{
public:
    // Deleted
    Reader() = delete;
    Reader(Reader&&) = delete;
    Reader(const Reader&) = delete;
    Reader& operator=(Reader&&) = delete;
    Reader& operator=(const Reader&) = delete;

    /**
     * Constructor.
     *
     * @param buffer Buffer.
     */
    explicit Reader(BytesView buffer);

    /**
     * Destructor.
     */
    virtual ~Reader();

    /**
     * Read int16.
     *
     * @return Value.
     */
    [[nodiscard]]
    std::int16_t readInt16();

    /**
     * Read int32.
     *
     * @return Value.
     */
    [[nodiscard]]
    std::int32_t readInt32();

    /**
     * Read int64 number.
     *
     * @return Value.
     */
    [[nodiscard]]
    std::int64_t readInt64();

    /**
     * Read string.
     *
     * @return String value.
     */
    [[nodiscard]]
    std::string readString();

    /**
     * Read GUID.
     *
     * @return GUID value.
     */
    [[nodiscard]]
    Guid readGuid();

    /**
     * If the next value is Nil, read it and move reader to the next position.
     *
     * @return @c true if the value was nil.
     */
    bool tryReadNil();

    /**
     * Skip next value.
     */
    void skip();

private:
    /**
     * Move to the next value.
     */
    void next();

    /** Buffer. */
    BytesView m_buffer;

    /** Unpacked data. */
    msgpack_unpacked m_unpacked;

    /** Result of the last move operation. */
    msgpack_unpack_return m_moveRes;
};

} // namespace ignite::protocol
