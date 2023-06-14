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

#include "ignite/protocol/writer.h"

namespace ignite::protocol {

int writer::write_callback(void *data, const char *buf, size_t len) {
    if (!data)
        return 0;

    auto buffer = static_cast<buffer_adapter *>(data);

    // We do not support messages larger than MAX_INT32
    if (buffer->data().size() + len > std::size_t(std::numeric_limits<int32_t>::max()))
        return -1;

    auto bytes = reinterpret_cast<const std::byte *>(buf);
    buffer->write_raw(bytes_view{bytes, len});

    return 0;
}

} // namespace ignite::protocol
