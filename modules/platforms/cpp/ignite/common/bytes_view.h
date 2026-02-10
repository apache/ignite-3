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

#include "detail/byte_traits.h"

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace ignite {

/**
 * @brief Wrapper for raw bytes.
 *
 * Provides convenient methods for managing data in binary format.
 */
struct bytes_view : std::basic_string_view<std::byte, detail::byte_traits> {
    using base_type = std::basic_string_view<std::byte, detail::byte_traits>;

    constexpr bytes_view() noexcept = default;

    constexpr bytes_view(const std::byte *data, std::size_t size) noexcept
        : base_type(data, size) {}

    constexpr bytes_view(const void *data, std::size_t size) noexcept
        : base_type(static_cast<const std::byte *>(data), size) {}

    constexpr bytes_view(const base_type &v) noexcept // NOLINT(google-explicit-constructor)
        : base_type(v.data(), v.size()) {}

    template<std::size_t SIZE>
    constexpr bytes_view(const char (&v)[SIZE]) noexcept // NOLINT(google-explicit-constructor)
        : base_type(reinterpret_cast<const std::byte *>(v), SIZE) {}

    template<std::size_t SIZE>
    constexpr bytes_view(const std::array<std::byte, SIZE> &v) noexcept // NOLINT(google-explicit-constructor)
        : base_type(v.data(), v.size()) {}

    bytes_view(const std::string &v) noexcept // NOLINT(google-explicit-constructor)
        : base_type(reinterpret_cast<const std::byte *>(v.data()), v.size()) {}

    bytes_view(const std::string_view &v) noexcept // NOLINT(google-explicit-constructor)
        : base_type(reinterpret_cast<const std::byte *>(v.data()), v.size()) {}

    bytes_view(const std::vector<std::byte> &v) noexcept // NOLINT(google-explicit-constructor)
        : base_type(v.data(), v.size()) {}

    explicit operator std::string() const { return {reinterpret_cast<const char *>(data()), size()}; }

    explicit operator std::vector<std::byte>() const { return {begin(), end()}; }
};

} // namespace ignite
