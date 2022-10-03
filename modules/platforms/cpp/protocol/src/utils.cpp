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

#include <mutex>
#include <random>
#include <sstream>

#include "ignite/protocol/utils.h"

namespace ignite::protocol {

Guid makeRandomGuid() {
    static std::mutex randomMutex;
    static std::random_device rd;
    static std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib;

    std::lock_guard<std::mutex> lock(randomMutex);

    return {distrib(gen), distrib(gen)};
}

std::optional<ignite_error> readError(Reader &reader) {
    if (reader.tryReadNil())
        return std::nullopt;

    Guid traceId = reader.tryReadNil() ? makeRandomGuid() : reader.readGuid();
    int32_t code = reader.tryReadNil() ? 65537 : reader.readInt32();
    std::string className = reader.readString();
    std::string message = reader.readString();

    std::stringstream errMsgBuilder;

    errMsgBuilder << className << ": " << message << " (" << code << ", " << traceId << ")";

    return {ignite_error(status_code(code), errMsgBuilder.str())};
}

} // namespace ignite::protocol
