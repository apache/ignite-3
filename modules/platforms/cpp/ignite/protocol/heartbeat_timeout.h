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

#include <ignite/common/detail/duration_min_max.h>

#include <cassert>
#include <chrono>

namespace ignite {

inline std::chrono::milliseconds calculate_heartbeat_interval(std::chrono::milliseconds config_value,
    std::chrono::milliseconds idle_timeout) {
    static const std::chrono::milliseconds MIN_HEARTBEAT_INTERVAL = std::chrono::milliseconds(500);

    if (config_value.count()) {
        assert(config_value.count() > 0);

        config_value = min(idle_timeout / 3, config_value);
        config_value = max(MIN_HEARTBEAT_INTERVAL, config_value);
    }

    return config_value;
}

} // namespace ignite
