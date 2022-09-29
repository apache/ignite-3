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

#include "BinaryTupleSchema.h"
#include "common/Platform.h"
#include "common/Types.h"

#include <cassert>
#include <cstring>
#include <limits>
#include <type_traits>

namespace ignite {

/**
 * @brief Binary tuple builder.
 *
 * A tuple builder is used to create one or more binary tuples for a given schema.
 *
 * Building a tuple takes two passes. On the first pass the builder finds the tuple
 * layout. On the second pass it actually fills the tuple data.
 *
 * More precisely a tuple is built with the following call sequence:
 *
 * 1. Initialize the builder with the @ref start call.
 * 2. Supply all elements with one or more @ref claim calls in the order that
 *    corresponds to the tuple schema.
 * 3. Determine tuple layout with the @ref layout call.
 * 4. Supply all elements again with one or more @ref append calls in the same
 *    order with the same values.
 * 5. Finally, the resulting binary tuple is obtained with the @ref build call.
 */
class BinaryTupleBuilder {
    const IntT elementCount; /**< Total number of elements. */

    IntT elementIndex; /**< Index of the next element to add. */

    IntT nullElements; /**< The number of null elements. */

    SizeT valueAreaSize; /**< Total size of all values. */

    SizeT entrySize; /**< Size of an offset table entry. */

    std::byte *nextEntry; /**< Position for the next offset table entry. */

    std::byte *valueBase; /**< Position of the value area.*/

    std::byte *nextValue; /**< Position for the next value. */

    std::vector<std::byte> binaryTuple; /**< Internal buffer for tuple generation. */

public:
    /**
     * @brief Constructs a new Tuple Builder object.
     *
     * @param schema Binary tuple schema.
     */
    explicit BinaryTupleBuilder(IntT elementCount) noexcept;

    /**
     * @brief Starts a new tuple.
     */
    void start() noexcept;

    /**
     * @brief Assigns a null value for the next element.
     */
    void claim(std::nullopt_t /*null*/) noexcept {
        assert(elementIndex < elementCount);
        nullElements++;
        elementIndex++;
    }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param valueSize required size for the value
     */
    void claim(SizeT valueSize) noexcept {
        assert(elementIndex < elementCount);
        valueAreaSize += valueSize;
        elementIndex++;
    }

    /**
     * @brief Assigns a binary value for the next element.
     *
     * @param type Element type.
     * @param bytes Binary element value.
     */
    void claim(DATA_TYPE type, const BytesView &bytes) noexcept { claim(sizeOf(type, bytes)); }

    /**
     * @brief Assigns a value or null for the next element.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param type Element type.
     * @param slice Optional value of an internal tuple field.
     */
    template <typename BytesT>
    void claim(DATA_TYPE type, const std::optional<BytesT> &slice) noexcept {
        if (slice.has_value()) {
            claim(type, slice.value());
        } else {
            claim(std::nullopt);
        }
    }

    /**
     * @brief Assigns values for a number of elements.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param schema Tuple schema.
     * @param tuple Tuple in the internal form.
     */
    template <typename BytesT>
    void claim(const BinaryTupleSchema &schema, const std::vector<std::optional<BytesT>> &tuple) noexcept {
        for (IntT i = 0; i < schema.numElements(); i++) {
            claim(schema.getElement(i).dataType, tuple[i]);
        }
    }

    /**
     * @brief Performs binary tuple layout.
     */
    void layout();

    /**
     * @brief Appends a null value for the next element.
     */
    void append(std::nullopt_t /*null*/) {
        assert(nullElements > 0);
        assert(elementIndex < elementCount);
        binaryTuple[BinaryTupleSchema::getNullOffset(elementIndex)] |= BinaryTupleSchema::getNullMask(elementIndex);
        appendEntry();
    }

    /**
     * @brief Appends a value for the next element.
     *
     * @param type Element type.
     * @param value Value of an internal tuple field.
     */
    void append(DATA_TYPE type, const BytesView &bytes);

    /**
     * @brief Appends a value or null for the next element.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param type Element type.
     * @param slice Optional value of an internal tuple field.
     */
    template <typename BytesT>
    void append(DATA_TYPE type, const std::optional<BytesT> &slice) {
        if (slice.has_value()) {
            append(type, slice.value());
        } else {
            append(std::nullopt);
        }
    }

    /**
     * @brief Appends values for a number of elements.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param schema Tuple schema.
     * @param tuple Tuple in the internal form.
     */
    template <typename BytesT>
    void append(const BinaryTupleSchema &schema, const std::vector<std::optional<BytesT>> &tuple) {
        for (IntT i = 0; i < schema.numElements(); i++) {
            append(schema.getElement(i).dataType, tuple[i]);
        }
    }

    /**
     * @brief Finalizes and returns a binary tuple.
     *
     * @return Byte buffer with binary tuple.
     */
    const std::vector<std::byte> &build() {
        assert(elementIndex == elementCount);
        return binaryTuple;
    }

    /**
     * @brief Builds a binary tuple from an internal tuple representation.
     *
     * @tparam BytesT Byte container for a single internal tuple field.
     * @param schema Tuple schema.
     * @param tuple Tuple in the internal form.
     * @return Byte buffer with binary tuple.
     */
    template <typename BytesT>
    const std::vector<std::byte> &build(
        const BinaryTupleSchema &schema, const std::vector<std::optional<BytesT>> &tuple) {
        start();
        claim(schema, tuple);
        layout();
        append(schema, tuple);
        return build();
    }

private:
    /**
     * @brief Checks if a value of a given integer type can be compressed to a smaller integer type.
     *
     * @tparam T Source integer type.
     * @tparam U Target integer type.
     * @param value Source value.
     * @return true If the source value can be compressed.
     * @return false If the source value cannot be compressed.
     */
    template <typename T, typename U>
    static bool fits(T value) noexcept {
        static_assert(std::is_signed_v<T>);
        static_assert(std::is_signed_v<U>);
        using V = std::make_unsigned_t<U>;
        return (std::make_unsigned_t<T>(value) + std::numeric_limits<U>::max() + 1) <= std::numeric_limits<V>::max();
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static SizeT sizeOfInt8(std::int8_t value) noexcept { return value == 0 ? 0 : sizeof(std::int8_t); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static SizeT sizeOfInt16(std::int16_t value) noexcept {
        if (fits<std::int16_t, std::int8_t>(value)) {
            return sizeOfInt8(std::int8_t(value));
        }
        return sizeof(std::int16_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static SizeT sizeOfInt32(std::int32_t value) noexcept {
        if (fits<std::int32_t, std::int8_t>(value)) {
            return sizeOfInt8(std::int8_t(value));
        }
        if (fits<std::int32_t, std::int16_t>(value)) {
            return sizeof(std::int16_t);
        }
        return sizeof(std::int32_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static SizeT sizeOfInt64(std::int64_t value) noexcept {
        if (fits<std::int64_t, std::int16_t>(value)) {
            return sizeOfInt16(std::int16_t(value));
        }
        if (fits<std::int64_t, std::int32_t>(value)) {
            return sizeof(std::int32_t);
        }
        return sizeof(std::int64_t);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static SizeT sizeOfFloat(float value) noexcept { return value == 0.0f ? 0 : sizeof(float); }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param value Actual element value.
     * @return Required size.
     */
    static SizeT sizeOfDouble(double value) noexcept {
        float floatValue = static_cast<float>(value);
        return floatValue == value ? sizeOfFloat(floatValue) : sizeof(double);
    }

    /**
     * @brief Computes required binary size for a given value.
     *
     * @param type Element type.
     * @param bytes Binary element value.
     * @return Required size.
     */
    static SizeT sizeOf(DATA_TYPE type, BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * @param bytes Binary element value.
     */
    void putBytes(BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void putInt8(BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void putInt16(BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void putInt32(BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void putInt64(BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void putFloat(BytesView bytes);

    /**
     * @brief Writes binary value of specified element.
     *
     * The written value may differ from the original because of value compression.
     *
     * @param bytes Binary element value.
     */
    void putDouble(BytesView bytes);

    /**
     * @brief Adds an entry to the offset table.
     */
    void appendEntry() {
        uint64_t offset = nextValue - valueBase;
        static_assert(platform::ByteOrder::littleEndian);
        assert(nextEntry + entrySize <= valueBase);
        std::memcpy(nextEntry, &offset, entrySize);
        nextEntry += entrySize;
        elementIndex++;
    }
};

} // namespace ignite
