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

#include "ignite/protocol/reader.h"

#include <ignite/protocol/utils.h>

namespace ignite::protocol {

reader::reader(bytes_view buffer)
    : m_buffer(buffer)
    , m_current_val()
    , m_move_res(MSGPACK_UNPACK_SUCCESS) {

    msgpack_unpacked_init(&m_current_val);

    next();
}

bool reader::try_read_nil() {
    if (m_current_val.data.type != MSGPACK_OBJECT_NIL)
        return false;

    next();
    return true;
}

void reader::next() {
    check_data_in_stream();

    m_offset = m_offset_next;
    m_move_res = msgpack_unpack_next(
        &m_current_val, reinterpret_cast<const char *>(m_buffer.data()), m_buffer.size(), &m_offset_next);
}

} // namespace ignite::protocol
