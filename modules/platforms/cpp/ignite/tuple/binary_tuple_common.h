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

namespace ignite {

/** C++ version of Java int. Used as a column number, etc. */
using tuple_num_t = std::int32_t;

/** Data size for columns and entire rows too. */
using tuple_size_t = std::uint32_t;

namespace binary_tuple_common {

/** Header size in bytes. */
constexpr tuple_size_t HEADER_SIZE = 1;

/** Mask for null-map flag. */
constexpr std::byte NULLMAP_FLAG{0b100};

/** Mask for tuple size bits. */
constexpr std::byte VARLEN_ENTRY_SIZE_MASK{0b11};

/** Encodes size as a bit mask. */
constexpr unsigned int size_to_flags(tuple_size_t size) noexcept {
    if (size <= UINT8_MAX) {
        return 0b00;
    }
    if (size <= UINT16_MAX) {
        return 0b01;
    }
    return 0b10;
}

/**
 * @brief A helper to work with binary tuple header.
 */
struct header {
    /** Tuple flags. */
    std::byte flags{0};

    /** Sets the size of offset-table entries based on the value area size. */
    unsigned int set_entry_size(tuple_size_t value_area_size) noexcept {
        const unsigned size_log2 = size_to_flags(value_area_size);
        flags &= ~VARLEN_ENTRY_SIZE_MASK;
        flags |= std::byte(size_log2);
        return 1u << size_log2;
    }

    /** Gets the size of a single offset-table entry, in bytes. */
    tuple_size_t get_entry_size() const noexcept { return 1u << static_cast<unsigned>(flags & VARLEN_ENTRY_SIZE_MASK); }

    /** Sets the nullmap flag on. */
    void set_nullmap_flag() noexcept { flags |= NULLMAP_FLAG; }

    /** Gets the nullmap flag value. */
    bool get_nullmap_flag() const noexcept { return (flags & NULLMAP_FLAG) != std::byte{0}; }
};

/**
 * @brief Gets the nullmap size.
 *
 * @return Nullmap size in bytes.
 */
constexpr tuple_size_t get_nullmap_size(tuple_num_t num_elements) noexcept { return (num_elements + 7) / 8; }

/**
 * @brief Gets offset of the byte that contains null-bit of a given tuple element.
 *
 * @param index Tuple element index.
 * @return Offset of the required byte relative to the tuple start.
 */
constexpr tuple_size_t get_null_offset(tuple_num_t index) noexcept { return HEADER_SIZE + index / 8; }

/**
 * @brief Gets a null-bit mask corresponding to a given tuple element.
 *
 * @param index Tuple element index.
 * @return Mask to extract the required null-bit.
 */
constexpr std::byte get_null_mask(tuple_num_t index) noexcept { return std::byte{1} << (index % 8); }

} // namespace binary_tuple_common

} // namespace ignite
