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
#include <sstream>
#include <cstdint>
#include <iomanip>

namespace ignite {

/** Cluster version. */
struct cluster_version {
    std::uint8_t major {};
    std::uint8_t minor {};
    std::uint8_t maintenance {};
    std::optional<std::uint8_t> patch {};
    std::optional<std::string> pre_release {};

    [[nodiscard]] std::string to_string() const {
        std::stringstream ss;

        ss << std::setfill('0') << std::setw(2) << int(major) << "."
           << std::setfill('0') << std::setw(2) << int(minor) << "."
           << std::setfill('0') << std::setw(4) << int(maintenance);

        if(patch) {
            ss << " " << int(patch.value());
        }

        if(pre_release) {
            ss << " " << pre_release.value();
        }

        return ss.str();
    }
};

} // namespace ignite
