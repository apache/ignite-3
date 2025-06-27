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

#include "ignite/protocol/buffer_adapter.h"

#include "ignite/common/detail/bytes.h"
#include <ignite/common/ignite_error.h>
#include <ignite/protocol/utils.h>

namespace ignite::protocol {

void buffer_adapter::write_length_header() {
    if (m_length_pos == std::numeric_limits<std::size_t>::max() || m_length_pos + LENGTH_HEADER_SIZE > m_buffer.size())
        throw ignite_error("Length header was not reserved properly in buffer");

    auto length = std::int32_t(m_buffer.size() - (m_length_pos + LENGTH_HEADER_SIZE));

    detail::bytes::store<detail::endian::BIG, int32_t>(m_buffer.data() + m_length_pos, length);
}

} // namespace ignite::protocol
