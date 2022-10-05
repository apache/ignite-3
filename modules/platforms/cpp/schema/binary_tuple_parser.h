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

#include "big_decimal.h"
#include "big_integer.h"
#include "binary_tuple_schema.h"
#include "ignite_date.h"
#include "ignite_date_time.h"
#include "ignite_time.h"
#include "ignite_timestamp.h"

#include "../common/types.h"
#include "../common/uuid.h"

namespace ignite {

/**
 * @brief Binary tuple parser.
 *
 * A tuple parser is used to parse a binary tuple with a given schema.
 */
class binary_tuple_parser {
    bytes_view binary_tuple; /**< The binary tuple to parse. */

    const IntT element_count; /**< Total number of elements. */

    IntT element_index; /**< Index of the next element to parse. */

    bool has_nullmap; /**< Flag that indicates if the tuple contains a nullmap. */

    SizeT entry_size; /**< Size of an offset table entry. */

    const std::byte *next_entry; /**< Position of the next offset table entry. */

    const std::byte *value_base; /**< Position of the value area. */

    const std::byte *next_value; /**< Position of the next value. */

public:
    /**
     * @brief Constructs a new parser object.
     *
     * The @ref data buffer may contain more binary data than single binary tuple.
     * The parser finds where the tuple ends and reads data only up to this point.
     * The @ref get_tuple() and @ref get_size() methods let know the actual tuple
     * size.
     *
     * @param num_elements Number of tuple elements.
     * @param data Binary tuple buffer.
     */
    explicit binary_tuple_parser(IntT num_elements, bytes_view data);

    /**
     * @brief Gets the original binary tuple.
     *
     * @return BytesView Binary tuple.
     */
    bytes_view get_tuple() const noexcept { return binary_tuple; }

    /**
     * @brief Gets the binary tuple size in bytes.
     *
     * @return Tuple size.
     */
    SizeT get_size() const noexcept { return static_cast<SizeT>(binary_tuple.size()); }

    /**
     * @brief Gets the expected total number of tuple elements.
     *
     * @return Number of elements.
     */
    IntT num_elements() const noexcept { return element_count; }

    /**
     * @brief Gets the number of parsed tuple elements.
     *
     * @return Number of parsed elements.
     */
    IntT num_parsed_elements() const noexcept { return element_index; }

    /**
     * @brief Gets the next value of the tuple.
     *
     * @return The next value.
     */
    element_view get_next();

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
    key_tuple_view parse_key(IntT num = NO_NUM);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int8_t get_int8(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int16_t get_int16(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int32_t get_int32(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static std::int64_t get_int64(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static float get_float(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static double get_double(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static big_integer get_number(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static uuid get_uuid(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static ignite_date get_date(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static ignite_time get_time(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static ignite_date_time get_date_time(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static ignite_timestamp get_timestamp(bytes_view bytes);
};

} // namespace ignite
