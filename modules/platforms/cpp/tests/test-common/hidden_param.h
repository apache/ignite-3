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
#include <type_traits>
#include <string>
#include <tuple>

#include <gtest/gtest.h>

struct hidden {
    explicit hidden(std::string val)
        : value { std::move(val) } {
    }

    [[nodiscard]] std::string get() const { return value; }

private:
    std::string value;
};

template<typename T>
auto unhide(const T& value) {
    if constexpr (std::is_same_v<T, hidden>) {
        return value.get();
    } else {
        return value;
    }
}

template<typename... Ts, std::size_t... I>
auto unhide_internal(const std::tuple<Ts...>& values, std::index_sequence<I...>) {
    return std::make_tuple(unhide(std::get<I>(values))...);
}

template<typename... Ts>
auto unhide(const std::tuple<Ts...>& values) {
    return unhide_internal(values, std::make_index_sequence<sizeof...(Ts)>{});
}

template<typename T>
std::string print_test_index(const testing::TestParamInfo<typename T::ParamType>& info) {
    return "_" + std::to_string(info.index);
}

template <>
class testing::internal::UniversalPrinter<hidden> {
public:
    static void Print(const hidden& /*val*/, ::std::ostream* os) {
        *os << "<hidden>";
    }
};
