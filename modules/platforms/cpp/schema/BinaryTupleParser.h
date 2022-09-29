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
#include "common/types.h"

namespace ignite {

/**
 * @brief Binary tuple parser.
 *
 * A tuple parser is used to parse a binary tuple with a given schema.
 */
class BinaryTupleParser {
    bytes_view binaryTuple; /**< The binary tuple to parse. */

    const IntT elementCount; /**< Total number of elements. */

    IntT elementIndex; /**< Index of the next element to parse. */

    bool hasNullmap; /**< Flag that indicates if the tuple contains a nullmap. */

    SizeT entrySize; /**< Size of an offset table entry. */

    const std::byte *nextEntry; /**< Position of the next offset table entry. */

    const std::byte *valueBase; /**< Position of the value area. */

    const std::byte *nextValue; /**< Position of the next value. */

public:
    /**
     * @brief Constructs a new parser object.
     *
     * The @ref data buffer may contain more binary data than single binary tuple.
     * The parser finds where the tuple ends and reads data only up to this point.
     * The @ref getTuple() and @ref getSize() methods let know the actual tuple
     * size.
     *
     * @param numElements Number of tuple elements.
     * @param data Binary tuple buffer.
     */
    explicit BinaryTupleParser(IntT numElements, bytes_view data);

    /**
     * @brief Gets the original binary tuple.
     *
     * @return BytesView Binary tuple.
     */
    bytes_view getTuple() const noexcept { return binaryTuple; }

    /**
     * @brief Gets the binary tuple size in bytes.
     *
     * @return Tuple size.
     */
    SizeT getSize() const noexcept { return static_cast<SizeT>(binaryTuple.size()); }

    /**
     * @brief Gets the expected total number of tuple elements.
     *
     * @return Number of elements.
     */
    IntT numElements() const noexcept { return elementCount; }

    /**
     * @brief Gets the number of parsed tuple elements.
     *
     * @return Number of parsed elements.
     */
    IntT numParsedElements() const noexcept { return elementIndex; }

    /**
     * @brief Gets the next value of the tuple.
     *
     * @return The next value.
     */
    element_view getNext();

    /**
     * @brief Gets a series of values.
     *
     * @param num Required number of values. The value of NO_NUM means all the available values.
     * @return A set of values.
     */
    tuple_view parse(IntT num = NO_NUM);

    /**
     * @brief Gets a series of values presuming they belong to a key. So no NULL values are allowed.
     *
     * @param num Required number of values. The value of NO_NUM means all the available values.
     * @return A set of values.
     */
    key_tuple_view parseKey(IntT num = NO_NUM);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int8_t getInt8(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int16_t getInt16(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int32_t getInt32(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int64_t getInt64(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static float getFloat(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static double getDouble(bytes_view bytes);
};

} // namespace ignite
