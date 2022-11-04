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

#include <cstddef>
#include <string_view>
#include <vector>

namespace ignite {

/** A slice of raw bytes. */
struct bytes_view : std::basic_string_view<std::byte> {
    using Base = std::basic_string_view<std::byte>;

    constexpr bytes_view() noexcept = default;

    constexpr bytes_view(const std::byte *data, std::size_t size) noexcept
        : Base(data, size) {}

    constexpr bytes_view(const Base &v) noexcept // NOLINT(google-explicit-constructor)
        : Base(v.data(), v.size()) {}

    bytes_view(const std::vector<std::byte> &v) noexcept // NOLINT(google-explicit-constructor)
        : Base(v.data(), v.size()) {}

    explicit operator std::vector<std::byte>() const { return {begin(), end()}; }
};

} // namespace ignite
