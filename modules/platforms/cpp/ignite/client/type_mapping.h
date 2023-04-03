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

namespace ignite {

template<typename T>
ignite_tuple convert_to_tuple(T&& value);

template<typename T>
T convert_from_tuple(ignite_tuple&& value);

template<typename T>
ignite_tuple convert_to_tuple(const T& value) {
    return convert_to_tuple(T(value));
}

template<typename T>
std::optional<ignite_tuple> convert_to_tuple(std::optional<T>&& value) {
    if (!value.has_value())
        return std::nullopt;

    return {convert_to_tuple<T>(*std::move(value))};
}

template<typename T>
std::optional<T> convert_from_tuple(std::optional<ignite_tuple>&& value) {
    if (!value.has_value())
        return std::nullopt;

    return {convert_from_tuple<T>(*std::move(value))};
}

template<typename T>
ignite_result<std::optional<T>> convert_from_tuple(ignite_result<std::optional<ignite_tuple>>&& value) {
    if (value.has_error())
        return {std::move(value).error()};

    return {convert_from_tuple<T>(std::move(value).value())};
}

} // namespace ignite
