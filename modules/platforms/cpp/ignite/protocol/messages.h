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

#pragma once

#include "ignite/protocol/protocol_context.h"
#include "ignite/protocol/protocol_version.h"

#include "ignite/common/bytes_view.h"
#include "ignite/common/ignite_error.h"

#include <map>
#include <vector>

namespace ignite::protocol {

/**
 * Response flags.
 */
enum class response_flag : std::int32_t {
    /// Partition assignment changed in cluster.s
    PARTITION_ASSIGNMENT_CHANGED = 1,

    /// Notification flag.
    NOTIFICATION_FLAG = 2,

    /// Error flag.
    ERROR_FLAG = 4,
};

/**
 * Test whether flag is set.
 *
 * @param flags Flags.
 * @param to_test A specific flag to test.
 * @return @c true if the flag is set.
 */
inline bool test_flag(std::int32_t flags, response_flag to_test) {
    return (flags & std::int32_t(to_test)) != 0;
}

/**
 * Handshake response.
 */
struct handshake_response {
    /** Error. */
    std::optional<ignite_error> error{};

    /** Protocol context. */
    protocol_context context{};

    /** Observable timestamp. */
    int64_t observable_timestamp;
};

/**
 * Make handshake request.
 *
 * @param client_type Client type.
 * @param ver Protocol version.
 * @param extensions Extensions.
 * @return Message.
 */
std::vector<std::byte> make_handshake_request(
    std::int8_t client_type, protocol_version ver, std::map<std::string, std::string> extensions);

/**
 * Parse handshake response.
 *
 * @param message Message to parse.
 * @return Handshake response.
 */
handshake_response parse_handshake_response(bytes_view message);

} // namespace ignite::protocol
