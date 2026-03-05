/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
struct partition_assignment {
    /** Assignment timestamp. */
    volatile std::int64_t timestamp{0};

    /** Partitions. */
    std::vector<std::optional<std::string>> partitions;

    /**
     * Check whether the assignment is outdated.
     *
     * @param actual_timestamp Timestamp.
     * @return @c true if assignment is outdated.
     */
    [[nodiscard]] bool is_outdated(std::int64_t actual_timestamp) const { return timestamp < actual_timestamp; }
};

} // namespace ignite::protocol
