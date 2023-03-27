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

#include "binary_tuple_builder.h"
#include "binary_tuple_schema.h"

#include <cstring>
#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>

namespace ignite {

/**
 * This is just a helper for unit tests.
 */
class tuple_assembler {
private:
    binary_tuple_schema schema;

    binary_tuple_builder builder;

public:
    /** Constructs a new Tuple Assembler object. */
    explicit tuple_assembler(binary_tuple_schema &&sch)
        : schema(std::move(sch))
        , builder(schema.num_elements()) {}

    /** Starts a new tuple. */
    void start() { builder.start(); }

    /** Claims a null value. */
    void claim_null() { builder.claim(std::nullopt); }

    /** Appends a null value. */
    void append_null() { builder.append(std::nullopt); }

    template<typename T>
    void claim_value(const T &value) {
        if constexpr (std::is_same<T, std::nullopt_t>::value) {
            builder.claim(value);
        } else if constexpr (std::is_same<T, int8_t>::value) {
            builder.claim_int8(value);
        } else if constexpr (std::is_same<T, int16_t>::value) {
            builder.claim_int16(value);
        } else if constexpr (std::is_same<T, int32_t>::value) {
            builder.claim_int32(value);
        } else if constexpr (std::is_same<T, int64_t>::value) {
            builder.claim_int64(value);
        } else if constexpr (std::is_same<T, float>::value) {
            builder.claim_float(value);
        } else if constexpr (std::is_same<T, double>::value) {
            builder.claim_double(value);
        } else if constexpr (std::is_same<T, std::string>::value) {
            builder.claim_string(value);
        }
    }

    template<typename T>
    void append_value(const T &value) {
        if constexpr (std::is_same<T, std::nullopt_t>::value) {
            builder.append(value);
        } else if constexpr (std::is_same<T, int8_t>::value) {
            builder.append_int8(value);
        } else if constexpr (std::is_same<T, int16_t>::value) {
            builder.append_int16(value);
        } else if constexpr (std::is_same<T, int32_t>::value) {
            builder.append_int32(value);
        } else if constexpr (std::is_same<T, int64_t>::value) {
            builder.append_int64(value);
        } else if constexpr (std::is_same<T, float>::value) {
            builder.append_float(value);
        } else if constexpr (std::is_same<T, double>::value) {
            builder.append_double(value);
        } else if constexpr (std::is_same<T, std::string>::value) {
            builder.append_string(value);
        }
    }

    /**
     * @brief Assembles and returns a tuple in binary format.
     *
     * @tparam Ts Types parameter pack.
     * @param tupleArgs Elements to be appended to column.
     * @return Byte buffer with tuple in the binary form.
     */
    template<typename... Ts>
    const auto &build(std::tuple<Ts...> const &tuple) {
        start();
        std::apply([&](auto &&...args) { ((claim_value(args)), ...); }, tuple);
        builder.layout();
        std::apply([&](auto &&...args) { ((append_value(args)), ...); }, tuple);
        return builder.build();
    }
};

} // namespace ignite
