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

#include "common/ignite_error.h"
#include "ignite/network/data_buffer.h"

namespace ignite::network
{

DataBuffer::DataBuffer() :
    m_position(0),
    m_length(0),
    m_data() { }

DataBuffer::DataBuffer(BytesView data) :
    m_position(0),
    m_length(int32_t(data.size())),
    m_data(data) { }

DataBuffer::DataBuffer(BytesView data, int32_t pos, int32_t len) :
    m_position(pos),
    m_length(len),
    m_data(data) { }

void DataBuffer::beConsumed(std::vector<std::byte>& dst, int32_t toCopy)
{
    int32_t end = m_position + toCopy;
    if (end > m_data.size())
        end = int32_t(m_data.size());

    dst.insert(dst.end(), m_data.begin() + m_position, m_data.begin() + end);
    advance(toCopy);
}

const int8_t *DataBuffer::getData() const
{
    return reinterpret_cast<const int8_t*>(m_data.data() + m_position);
}

int32_t DataBuffer::getSize() const
{
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

void DataBuffer::skip(int32_t bytes)
{
    int32_t toSkip = bytes < getSize() ? bytes : getSize();

    advance(toSkip);
}

BytesView DataBuffer::getBytesView() const
{
    return {reinterpret_cast<const std::byte *>(getData()), std::size_t(m_length)};
}

} // namespace ignite::network
