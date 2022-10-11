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

namespace ignite::protocol {

/**
 * Extension types.
 */
enum class extension_type : std::int8_t {
    NUMBER = 1,

    DECIMAL = 2,

    UUID = 3,

    DATE = 4,

    TIME = 5,

    DATE_TIME = 6,

    TIMESTAMP = 7,

    BITMASK = 8,

    NO_VALUE = 10
};

} // namespace ignite::protocol
