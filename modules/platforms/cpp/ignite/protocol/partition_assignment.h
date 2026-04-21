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

    /**
     * Constructor.
     *
     * @param timestamp Partition assignment data.
     * @param partitions Partition assignment encode into vector.
     */
    partition_assignment(std::int64_t timestamp, std::vector<std::optional<std::string>> partitions)
        : m_timestamp(timestamp)
        , m_partitions(std::move(partitions)) {}

    /**
     * Check whether the assignment is outdated.
     *
     * @param actual_timestamp Timestamp.
     * @return @c true if assignment is outdated.
     */
    [[nodiscard]] bool is_outdated(std::int64_t actual_timestamp) const { return m_timestamp < actual_timestamp; }

    /**
     * Partitions. Vector is decoded as following: size of collection is total number of partitions,
     * i-th value means that partition with id == i resides on (primary replica of that partition belongs to)
     * node with provided consisted id (AKA node name).
     *
     * @return Vector with partition assignments.
     */
    [[nodiscard]] const std::vector<std::optional<std::string>>& get_partitions() const {
        return m_partitions;
    }
private:
    /** Assignment timestamp. */
    std::int64_t m_timestamp{0};

    /**
    * Partitions. Vector is decoded as following: size of collection is total number of partitions,
    * i-th value means that partition with id == i resides on (primary replica of that partition belongs to)
    * node with provided consisted id (AKA node name).
    */
    std::vector<std::optional<std::string>> m_partitions;
};

} // namespace ignite::protocol
