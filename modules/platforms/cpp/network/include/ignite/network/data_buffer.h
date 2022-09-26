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
 *
 * Represents a consumable chunk of data.
 */
class DataBuffer
{
public:
    // Default
    DataBuffer() = default;
    ~DataBuffer() = default;
    DataBuffer(DataBuffer&&) = default;
    DataBuffer(const DataBuffer&) = default;
    DataBuffer& operator=(DataBuffer&&) = default;
    DataBuffer& operator=(const DataBuffer&) = default;

    /**
     * Constructor.
     *
     * @param data Data.
     */
    explicit DataBuffer(BytesView data) :
        m_data(data) { }

    /**
     * Constructor.
     *
     * @param data Data.
     * @param pos Start of data.
     * @param len Length.
     */
    DataBuffer(BytesView data, size_t pos, size_t len) :
        m_data(data.substr(pos, len)) { }

    /**
     * Consume buffer data by the vector.
     *
     * @param dst Vector to append data to.
     * @param bytes Number of bytes to consume.
     */
    void consumeBy(std::vector<std::byte>& dst, size_t bytes) {
        if (bytes > m_data.size())
            bytes = m_data.size();

        dst.insert(dst.end(), m_data.begin(), m_data.begin() + ptrdiff_t(bytes));
        skip(bytes);
    }

    /**
     * Check whether data buffer was fully consumed.
     *
     * @return @c true if the buffer is empty and @c false otherwise.
     */
    [[nodiscard]]
    bool isEmpty() const {
        return m_data.empty();
    }

    /**
     * Consume the whole buffer.
     *
     * @return Buffer containing consumed data.
     */
    DataBuffer consumeEntirely() {
        DataBuffer res(*this);
        m_data = {};

        return res;
    }

    /**
     * Skip specified number of bytes.
     *
     * @param bytes Bytes to skip.
     */
    void skip(size_t bytes) {
        if (bytes >= m_data.size())
            m_data = {};
        else
            m_data = m_data.substr(bytes);
    }

    /**
     * Get bytes view.
     *
     * @return Bytes view.
     */
    [[nodiscard]]
    BytesView getBytesView() const {
        return m_data;
    }

private:
    /** Data. */
    BytesView m_data;
};

} // namespace ignite::network
