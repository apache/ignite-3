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

#include "ignite/protocol/bitset_span.h"

#include <vector>
#include <cstddef>

namespace ignite::protocol {

/**
 * Protocol bitmask features.
 */
enum class bitmask_feature {
    /** Qualified name table requests. */
    TABLE_REQS_USE_QUALIFIED_NAME = 2,
};

/**
 * Get all supported bitmask features in binary form.
 *
 * @return Return all supported bitmask features in binary form.
 */
inline std::vector<std::byte> all_supported_bitmask_features() {
    std::vector<std::byte> res(1, std::byte{0});

    bitset_span span(res.data(), res.size());
    span.set(static_cast<std::size_t>(bitmask_feature::TABLE_REQS_USE_QUALIFIED_NAME));

    return res;
}

} // namespace ignite::protocol
