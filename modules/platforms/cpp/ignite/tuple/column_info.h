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

#include <ignite/common/ignite_type.h>

namespace ignite {

/**
 * @brief Basic column info.
 */
struct column_info {
    /** Data type of the values in this column. */
    ignite_type type;

    /** True if the column values may be NULL, false otherwise. */
    bool nullable;

    bool operator==(const column_info &other) const noexcept {
        return type == other.type && nullable == other.nullable;
    }

    bool operator!=(const column_info &other) const noexcept { return !(operator==(other)); }
};

} // namespace ignite
