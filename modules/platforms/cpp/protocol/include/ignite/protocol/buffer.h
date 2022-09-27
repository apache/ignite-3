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

#include <msgpack.h>

#include "common/Types.h"

namespace ignite::protocol
{

class Writer;

/**
 * Buffer.
 */
class Buffer
{
    friend class Writer;
public:
    /** Length header size in bytes. */
    static constexpr size_t LENGTH_HEADER_SIZE = 4;

    // Default
    virtual ~Buffer() = default;
    Buffer(Buffer&&) = default;
    Buffer(const Buffer& other) = default;
    Buffer& operator=(Buffer&&) = default;
    Buffer& operator=(const Buffer&) = default;

    /**
     * Constructor.
     */
    Buffer();

    /**
     * Constructor.
     *
     * @param data Data.
     */
    [[maybe_unused]]
    explicit Buffer(std::vector<std::byte> data);

    /**
     * Write raw data.
     *
     * @param data Data to write.
     */
    void writeRawData(BytesView data);

    /**
     * Get underlying data buffer view.
     *
     * @return Underlying data buffer view.
     */
    [[nodiscard]]
    BytesView getData() const
    {
        return m_buffer;
    }

    /**
     * Reserving space for length header.
     */
    void reserveLengthHeader();

    /**
     * Write buffer length to previously reserved position.
     */
    void writeLengthHeader();

    /**
     * Get underlying data. Leaves buffer empty.
     *
     * @return Underlying data.
     */
     [[nodiscard]]
    std::vector<std::byte> extractData() {
        m_lengthPos = 0;
        return std::move(m_buffer);
    }

private:
    /**
     * Write callback.
     *
     * @param data Pointer to the instance.
     * @param buf Data to write.
     * @param len Data length.
     * @return Write result.
     */
    static int writeCallback(void* data, const char* buf, size_t len);

    /** Buffer. */
    std::vector<std::byte> m_buffer;

    /** Length position. */
    std::size_t m_lengthPos;
};

} // namespace ignite::protocol
