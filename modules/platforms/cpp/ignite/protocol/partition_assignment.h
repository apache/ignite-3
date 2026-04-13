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

#include <optional>
#include <string>
#include <vector>
#include <cstdint>

namespace ignite::protocol {

/**
 * Partition assignment.
 */
class partition_assignment {
public:
    partition_assignment() = default;
    partition_assignment(const partition_assignment&) = default;
    partition_assignment(partition_assignment&&) noexcept = default;
    partition_assignment &operator=(const partition_assignment &other) = default;
    partition_assignment &operator=(partition_assignment &&other) noexcept = default;

    partition_assignment(std::int64_t timestamp, std::vector<std::optional<std::string>> partitions)
        : timestamp(timestamp)
        , partitions(std::move(partitions)) {}

    /**
     * Check whether the assignment is outdated.
     *
     * @param actual_timestamp Timestamp.
     * @return @c true if assignment is outdated.
     */
    [[nodiscard]] bool is_outdated(std::int64_t actual_timestamp) const { return timestamp < actual_timestamp; }

    [[nodiscard]] const std::vector<std::optional<std::string>>& get_partitions() const {
        return partitions;
    }
private:
    /** Assignment timestamp. */
    std::int64_t timestamp{0};

    /** Partitions. */
    std::vector<std::optional<std::string>> partitions;
};

} // namespace ignite::protocol
