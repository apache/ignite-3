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

#include "binary_tuple_common.h"

#include <ignite/common/big_decimal.h>
#include <ignite/common/big_integer.h>
#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_date.h>
#include <ignite/common/ignite_date_time.h>
#include <ignite/common/ignite_duration.h>
#include <ignite/common/ignite_period.h>
#include <ignite/common/ignite_time.h>
#include <ignite/common/ignite_timestamp.h>
#include <ignite/common/uuid.h>

namespace ignite {

/**
 * @brief Binary tuple parser.
 *
 * A tuple parser is used to parse a binary tuple with a given schema.
 */
class binary_tuple_parser {
    bytes_view binary_tuple; /**< The binary tuple to parse. */

    const tuple_num_t element_count; /**< Total number of elements. */

    tuple_num_t element_index; /**< Index of the next element to parse. */

    tuple_size_t entry_size; /**< Size of an offset table entry. */

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
    explicit binary_tuple_parser(tuple_num_t num_elements, bytes_view data);

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
    tuple_size_t get_size() const noexcept { return static_cast<tuple_size_t>(binary_tuple.size()); }

    /**
     * @brief Gets the expected total number of tuple elements.
     *
     * @return Number of elements.
     */
    tuple_num_t num_elements() const noexcept { return element_count; }

    /**
     * @brief Gets the number of parsed tuple elements.
     *
     * @return Number of parsed elements.
     */
    tuple_num_t num_parsed_elements() const noexcept { return element_index; }

    /**
     * @brief Gets the next value of the tuple.
     *
     * @return The next value.
     */
    bytes_view get_next();

    /**
     * @brief Reads value of a variable-length element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static bytes_view get_varlen(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static bool get_bool(bytes_view bytes);

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
    static big_decimal get_decimal(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @param scale Scale of the decimal.
     * @return Element value.
     */
    static big_decimal get_decimal(bytes_view bytes, std::int16_t scale);

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

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static ignite_period get_period(bytes_view bytes);

    /**
     * @brief Reads value of specified element.
     *
     * @param bytes Binary view of the element.
     * @return Element value.
     */
    static ignite_duration get_duration(bytes_view bytes);
};

} // namespace ignite
