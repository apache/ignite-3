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

#include <cstdint>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>

namespace ignite {

/** Server version. */
struct server_version {
    std::uint8_t major{};
    std::uint8_t minor{};
    std::uint8_t maintenance{};
    std::optional<std::uint8_t> patch{};
    std::optional<std::string> pre_release{};

    [[nodiscard]] std::string to_string() const {
        std::stringstream ss;

        ss << std::setfill('0') << std::setw(2) << int(major) << "." << std::setfill('0') << std::setw(2) << int(minor)
           << "." << std::setfill('0') << std::setw(4) << int(maintenance);

        if (patch) {
            ss << " " << int(patch.value());
        }

        if (pre_release) {
            ss << " " << pre_release.value();
        }

        return ss.str();
    }
};

} // namespace ignite
