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

void Writer::writeMessageToBuffer(Buffer &buffer, const std::function<void(Writer &)> &script) {
    buffer.reserveLengthHeader();

    protocol::Writer writer(buffer);
    script(writer);

    buffer.writeLengthHeader();
}

int Writer::writeCallback(void *data, const char *buf, size_t len) {
    if (!data)
        return 0;

    auto buffer = static_cast<Buffer*>(data);

    // We do not support messages larger than MAX_INT32
    if (buffer->getData().size() + len > std::numeric_limits<int32_t>::max())
        return -1;

    auto bytes = reinterpret_cast<const std::byte*>(buf);
    buffer->writeRawData(BytesView{bytes, len});

    return 0;
}

} // namespace ignite::protocol
