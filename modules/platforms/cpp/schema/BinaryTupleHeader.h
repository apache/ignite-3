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

#include "common/Types.h"

#include <cstddef>
#include <cstdint>

namespace ignite {

/**
 * @brief A helper to work with binary tuple header.
 */
struct BinaryTupleHeader {
    /** Header size in bytes. */
    static constexpr std::size_t SIZE = 1;

    /** Mask for tuple size bits. */
    static constexpr std::byte VARLEN_ENTRY_SIZE_MASK{0b11};

    /** Mask for null-map flag. */
    static constexpr std::byte NULLMAP_FLAG{0b100};

    /** Tuple flags. */
    std::byte flags{0};

    /** Encodes size as bit mask. */
    static constexpr unsigned int sizeToFlags(SizeT size) noexcept {
        if (size <= UINT8_MAX) {
            return 0b00;
        } else if (size <= UINT16_MAX) {
            return 0b01;
        } else {
            return 0b10;
        }
    }

    /** Sets the size of varlen-table entries. */
    unsigned int setVarLenEntrySize(SizeT varlenAreaSize) noexcept {
        const unsigned sizeLog2 = sizeToFlags(varlenAreaSize);
        flags &= ~VARLEN_ENTRY_SIZE_MASK;
        flags |= std::byte(sizeLog2);
        return 1u << sizeLog2;
    }

    /** Gets the size of a single varlen-table entry, in bytes. */
    SizeT getVarLenEntrySize() const noexcept { return 1u << static_cast<unsigned>(flags & VARLEN_ENTRY_SIZE_MASK); }

    /** Sets the nullmap flag on. */
    void setNullMapFlag() noexcept { flags |= NULLMAP_FLAG; }

    /** Gets the nullmap flag value. */
    bool getNullMapFlag() const noexcept { return (flags & NULLMAP_FLAG) != std::byte{0}; }
};

} // namespace ignite
