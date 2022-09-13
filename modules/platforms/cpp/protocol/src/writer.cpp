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

#include "ignite/protocol/writer.h"

namespace ignite::protocol
{

void Writer::writeMessageToBuffer(Buffer &buffer, const std::function<void(Writer &)> &script)
{
    buffer.reserveLengthHeader();

    protocol::Writer writer(buffer);
    script(writer);

    buffer.writeLengthHeader();
}

Writer::Writer(Buffer& buffer) :
    m_buffer(buffer),
    m_packer(msgpack_packer_new(&m_buffer, Buffer::writeCallback)) { }

Writer::~Writer()
{
    msgpack_packer_free(m_packer);
}

void Writer::write(int8_t value)
{
    msgpack_pack_int8(m_packer, value);
}

void Writer::write(int16_t value)
{
    msgpack_pack_int16(m_packer, value);
}

void Writer::write(int32_t value)
{
    msgpack_pack_int32(m_packer, value);
}

void Writer::write(int64_t value)
{
    msgpack_pack_int64(m_packer, value);
}

void Writer::write(std::string_view value)
{
    msgpack_pack_str_with_body(m_packer, value.data(), value.size());
}

void Writer::writeBinaryEmpty()
{
    msgpack_pack_bin(m_packer, 0);
}

void Writer::writeMapEmpty()
{
    msgpack_pack_map(m_packer, 0);
}

} // namespace ignite::protocol
