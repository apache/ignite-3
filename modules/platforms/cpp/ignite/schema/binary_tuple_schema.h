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

#include "binary_tuple_header.h"
#include "column_info.h"
#include "types.h"

#include <ignite/common/bytes_view.h>

#include <iterator>
#include <vector>

namespace ignite {

/**
 * Descriptor for building and parsing binary tuples.
 */
class binary_tuple_schema {
private:
    std::vector<column_info> elements; /**< Element info needed for tuple construction. */

    bool nullables = false;

public:
    binary_tuple_schema() noexcept = default;
    binary_tuple_schema(binary_tuple_schema &&other) noexcept = default;
    binary_tuple_schema &operator=(binary_tuple_schema &&other) noexcept = default;

    binary_tuple_schema(const binary_tuple_schema &other) = delete;
    binary_tuple_schema &operator=(const binary_tuple_schema &other) = delete;

    /**
     * @brief Constructs a new Binary Tuple Schema object.
     *
     * @tparam T column_info iterator.
     * @param begin
     * @param end
     */
    template<typename T>
    binary_tuple_schema(T begin, T end) {
        elements.reserve(std::distance(begin, end));
        for (; begin < end; begin++) {
            const column_info element = *begin;
            if (element.nullable) {
                nullables = true;
            }
            elements.push_back(element);
        }
    }

    /**
     * @brief Tests if there are any nullable elements.
     *
     * @return true If there is one or more nullable elements.
     * @return false If there are no nullable elements.
     */
    bool has_nullables() const noexcept { return nullables; }

    /**
     * @brief Gets total number of elements in tuple schema.
     *
     * @return Number of elements.
     */
    IntT num_elements() const noexcept { return static_cast<IntT>(elements.size()); }

    /**
     * @brief Gets element info.
     *
     * @param index Element number.
     * @return Element info.
     */
    const column_info &get_element(IntT index) const { return elements[index]; }

    /**
     * @brief Gets the nullmap size.
     *
     * @return Nullmap size in bytes.
     */
    static constexpr SizeT get_nullmap_size(IntT num_elements) noexcept { return (num_elements + 7) / 8; }

    /**
     * @brief Gets offset of the byte that contains null-bit of a given tuple element.
     *
     * @param index Tuple element index.
     * @return Offset of the required byte relative to the tuple start.
     */
    static constexpr SizeT get_null_offset(IntT index) noexcept { return binary_tuple_header::SIZE + index / 8; }

    /**
     * @brief Gets a null-bit mask corresponding to a given tuple element.
     *
     * @param index Tuple element index.
     * @return Mask to extract the required null-bit.
     */
    static constexpr std::byte get_null_mask(IntT index) noexcept { return std::byte{1} << (index % 8); }

    /**
     * @brief Checks if a null-bit is set for a given tuple element.
     *
     * Note that this doesn't check for the null-map presence. It has to be done before
     * calling this function.
     *
     * @param tuple Binary tuple.
     * @param index Tuple element index.
     * @return true If the required null-bit is set.
     * @return false If the required null-bit is clear.
     */
    static bool has_null(const bytes_view &tuple, IntT index) noexcept {
        return (tuple[get_null_offset(index)] & get_null_mask(index)) != std::byte{0};
    }
};

} // namespace ignite
