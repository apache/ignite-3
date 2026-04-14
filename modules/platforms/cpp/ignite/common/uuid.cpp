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

#include "uuid.h"

#include <random>
#include <mutex>

namespace ignite {

std::optional<uuid> uuid::from_string(const std::string &text) {
    if (text.length() != 36) {
        return {};
    }

    auto str = text.c_str();

    auto parse_chunk = [str](size_t beg, size_t end, uint64_t &out) -> bool {
        char *p;

        if (errno != 0) {
            errno = 0;
        }

        out = std::strtoull(str + beg, &p, 16);

        if (p != str + end || (*p != '-' && *p != '\0') || errno == ERANGE) {
            return false;
        }

        return true;
    };

    std::uint64_t msb1, msb2, msb3;
    std::uint64_t lsb1, lsb2;

    if (!parse_chunk(0, 8, msb1)) {
        return {};
    }

    if (!parse_chunk(9, 13, msb2)) {
        return {};
    }

    if (!parse_chunk(14, 18, msb3)) {
        return {};
    }

    if (!parse_chunk(19, 23, lsb1)) {
        return {};
    }

    if (!parse_chunk(24, 36, lsb2)) {
        return {};
    }

    uint64_t msb = msb1 << 32 | msb2 << 16 | msb3;
    uint64_t lsb = lsb1 << 48 | lsb2;

    return uuid{static_cast<int64_t>(msb), static_cast<int64_t>(lsb)};
}

uuid uuid::random() {
    static std::mutex random_mutex;
    static std::random_device rd;
    static std::mt19937 gen(rd());

    std::uniform_int_distribution<int64_t> distrib;

    std::lock_guard<std::mutex> lock(random_mutex);

    return {distrib(gen), distrib(gen)};
}

} // namespace ignite
