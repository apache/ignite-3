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

#include <cstring>

#include "common/ignite_error.h"
#include "ignite/network/data_buffer.h"

namespace ignite::network
{

DataBuffer::DataBuffer() :
    m_position(0),
    m_length(0),
    m_data() { }

DataBuffer::DataBuffer(std::shared_ptr<protocol::Buffer> data) :
    m_position(0),
    m_length(data->getLength()),
    m_data(std::move(data)) { }

DataBuffer::DataBuffer(std::shared_ptr<protocol::Buffer> data, int32_t pos, int32_t len) :
    m_position(pos),
    m_length(len),
    m_data(std::move(data)) { }

void DataBuffer::beConsumed(int8_t *dst, int32_t size)
{
    if (!size)
        return;

    if (size < 0)
        throw IgniteError("Codec error: Can not read negative number of bytes");

    if (getSize() < size)
        throw IgniteError("Codec error: Not enough data to read data from buffer");

    std::memcpy(dst, getData(), size);
    advance(size);
}

void DataBuffer::beConsumed(std::vector<std::byte>& dst, int32_t toCopy)
{
    auto bytes = m_data->getBuffer();
    dst.insert(dst.end(), bytes.begin(), bytes.begin() + toCopy);
}

const int8_t *DataBuffer::getData() const
{
    return m_data->getData() + m_position;
}

int32_t DataBuffer::getSize() const
{
    if (!m_data)
        return 0;

    return m_length - m_position;
}

bool DataBuffer::isEmpty() const
{
    return getSize() <= 0;
}

DataBuffer DataBuffer::consumeEntirely()
{
    DataBuffer res(*this);
    advance(getSize());

    return res;
}

void DataBuffer::advance(int32_t val)
{
    m_position += val;
}

DataBuffer DataBuffer::clone() const
{
    if (isEmpty())
        return {};

    auto copy = std::make_shared<protocol::Buffer>(*m_data);

    return {copy, 0, m_length};
}

void DataBuffer::skip(int32_t bytes)
{
    int32_t toSkip = bytes < getSize() ? bytes : getSize();

    advance(toSkip);
}

BytesView DataBuffer::getBytesView() const
{
    return {reinterpret_cast<const std::byte *>(getData()), std::size_t(getSize())};
}

} // namespace ignite::network
