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
#include <memory>

#include "common/Types.h"

#include "ignite/protocol/buffer.h"

namespace ignite::network
{

/**
 * Data buffer.
 */
class DataBuffer
{
public:
    /**
     * Default constructor.
     */
    DataBuffer();

    /**
     * Constructor.
     *
     * @param data Data.
     */
    explicit DataBuffer(BytesView data);

    /**
     * Constructor.
     *
     * @param data Data.
     * @param pos Start of data.
     * @param len Length.
     */
    DataBuffer(BytesView data, int32_t pos, int32_t len);

    /**
     * Destructor.
     */
    ~DataBuffer() = default;

    /**
     * Consume data from buffer to the vector.
     *
     * @param dst Vector to append data to.
     * @param size Number of bytes to copy.
     */
    void beConsumed(std::vector<std::byte>& dst, int32_t toCopy);

    /**
     * Get data pointer.
     *
     * @return Data.
     */
    [[nodiscard]]
    const int8_t* getData() const;

    /**
     * Get packet size.
     *
     * @return Packet size.
     */
    [[nodiscard]]
    int32_t getSize() const;

    /**
     * Check whether data buffer was fully consumed.
     *
     * @return @c true if consumed and @c false otherwise.
     */
    [[nodiscard]]
    bool isEmpty() const;

    /**
     * Consume the whole buffer.
     *
     * @return Buffer containing consumed data.
     */
    DataBuffer consumeEntirely();

    /**
     * Skip specified number of bytes.
     *
     * @param bytes Bytes to skip.
     */
    void skip(int32_t bytes);

    /**
     * Get bytes view.
     *
     * @return Bytes view.
     */
    [[nodiscard]]
    BytesView getBytesView() const;

private:
    /**
     * Advance position in packet by specified value.
     *
     * @param val Bytes to advance.
     */
    void advance(int32_t val);

    /** Position in current data. */
    int32_t m_position;

    /** Data length. */
    int32_t m_length;

    /** Data. */
    const BytesView m_data;
};

} // namespace ignite::network
