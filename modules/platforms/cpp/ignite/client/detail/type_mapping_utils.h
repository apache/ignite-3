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

#include <ignite/client/table/ignite_tuple.h>
#include <ignite/client/type_mapping.h>

#include <ignite/common/ignite_result.h>

namespace ignite {
/**
 * Convert values to tuples.
 * @param vals Values.
 * @return Tuples.
 */
template<typename T>
std::vector<ignite_tuple> values_to_tuples(std::vector<T> values) {
    // TODO: Optimize memory usage (IGNITE-19198)
    std::vector<ignite_tuple> tuples;
    tuples.reserve(values.size());
    for (auto &&value : std::move(values)) {
        tuples.push_back(convert_to_tuple(std::move(value)));
    }
    return tuples;
}

/**
 * Convert key-value pairs to tuples.
 * @param vals Values.
 * @return Tuples.
 */
template<typename K, typename V>
std::vector<std::pair<ignite_tuple, ignite_tuple>> values_to_tuples(std::vector<std::pair<K, V>> values) {
    // TODO: Optimize memory usage (IGNITE-19198)
    std::vector<std::pair<ignite_tuple, ignite_tuple>> tuples;
    tuples.reserve(values.size());
    for (auto &&pair : std::move(values)) {
        tuples.emplace_back(convert_to_tuple(std::move(pair.first)), convert_to_tuple(std::move(pair.second)));
    }
    return tuples;
}

/**
 * Tuples to values.
 * @param tuples Tuples.
 * @return Values.
 */
template<typename T>
std::vector<T> tuples_to_values(std::vector<ignite_tuple> tuples) {
    // TODO: Optimize memory usage (IGNITE-19198)
    std::vector<T> values;
    values.reserve(tuples.size());
    for (auto &&tuple : std::move(tuples)) {
        values.emplace_back(convert_from_tuple<T>(std::move(tuple)));
    }
    return values;
}

/**
 * Optional tuples to optional values.
 * @param tuples Tuples.
 * @return Values.
 */
template<typename T>
std::vector<std::optional<T>> tuples_to_values(std::vector<std::optional<ignite_tuple>> tuples) {
    // TODO: Optimize memory usage (IGNITE-19198)
    std::vector<std::optional<T>> values;
    values.reserve(tuples.size());
    for (auto &&tuple : std::move(tuples)) {
        values.emplace_back(convert_from_tuple<T>(std::move(tuple)));
    }
    return values;
}

/**
 * Convert result from tuple-based type to user type.
 * @param res Result to convert.
 * @return Converted result.
 */
template<typename T>
ignite_result<std::optional<T>> convert_result(ignite_result<std::optional<ignite_tuple>> &&res) {
    if (res.has_error())
        return {std::move(res).error()};

    return {convert_from_tuple<T>(std::move(res).value())};
}

/**
 * Convert result from tuple-based type to user type.
 * @param res Result to convert.
 * @return Converted result.
 */
template<typename T>
ignite_result<std::vector<std::optional<T>>> convert_result(
    ignite_result<std::vector<std::optional<ignite_tuple>>> &&res) {
    if (res.has_error())
        return {std::move(res).error()};

    return {tuples_to_values<T>(std::move(res).value())};
}

/**
 * Convert result from tuple-based type to user type.
 * @param res Result to convert.
 * @return Converted result.
 */
template<typename T>
ignite_result<std::vector<T>> convert_result(ignite_result<std::vector<ignite_tuple>> &&res) {
    if (res.has_error())
        return {std::move(res).error()};

    return {tuples_to_values<T>(std::move(res).value())};
}

} // namespace ignite
