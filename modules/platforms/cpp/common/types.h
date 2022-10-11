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
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace ignite {

/** C++ version of java int. Used as column number, etc. */
using IntT = int32_t;

/** Data size for columns and entire rows too. */
using SizeT = uint32_t;

/** Non-existent column/element number. */
static constexpr IntT NO_NUM = -1;

/** A slice of raw bytes. */
struct bytes_view : std::basic_string_view<std::byte> {
    using Base = std::basic_string_view<std::byte>;

    constexpr bytes_view() noexcept = default;

    constexpr bytes_view(const std::byte *data, std::size_t size) noexcept
        : Base(data, size) { }

    constexpr bytes_view(const Base &v) noexcept // NOLINT(google-explicit-constructor)
        : Base(v.data(), v.size()) { }

    bytes_view(const std::vector<std::byte> &v) noexcept // NOLINT(google-explicit-constructor)
        : Base(v.data(), v.size()) { }

    explicit operator std::vector<std::byte>() const { return {begin(), end()}; }
};

/** Binary value for a potentially nullable column. */
using element_view = std::optional<bytes_view>;

/** A set of binary values for a whole or partial row. */
using tuple_view = std::vector<element_view>;

/** A set of binary values for the key part of a row. */
using key_tuple_view = std::vector<bytes_view>;

} // namespace ignite
