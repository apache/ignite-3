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

#include "ignite/common/primitive.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

using namespace ignite;

template<typename T>
void check_primitive_type(ignite_type expected) {
    primitive val(T{});
    EXPECT_EQ(val.get_type(), expected);
}

TEST(primitive, get_column_type) {
    check_primitive_type<std::nullptr_t>(ignite_type::NIL);
    check_primitive_type<bool>(ignite_type::BOOLEAN);
    check_primitive_type<std::int8_t>(ignite_type::INT8);
    check_primitive_type<std::int16_t>(ignite_type::INT16);
    check_primitive_type<std::int32_t>(ignite_type::INT32);
    check_primitive_type<std::int64_t>(ignite_type::INT64);
    check_primitive_type<float>(ignite_type::FLOAT);
    check_primitive_type<double>(ignite_type::DOUBLE);
    check_primitive_type<big_decimal>(ignite_type::DECIMAL);
    check_primitive_type<ignite_date>(ignite_type::DATE);
    check_primitive_type<ignite_time>(ignite_type::TIME);
    check_primitive_type<ignite_date_time>(ignite_type::DATETIME);
    check_primitive_type<ignite_timestamp>(ignite_type::TIMESTAMP);
    check_primitive_type<ignite_period>(ignite_type::PERIOD);
    check_primitive_type<ignite_duration>(ignite_type::DURATION);
    check_primitive_type<uuid>(ignite_type::UUID);
    check_primitive_type<std::string>(ignite_type::STRING);
    check_primitive_type<std::vector<std::byte>>(ignite_type::BYTE_ARRAY);
}

TEST(primitive, null_value_by_nullptr) {
    primitive val(nullptr);
    EXPECT_EQ(val.get_type(), ignite_type::NIL);
    EXPECT_TRUE(val.is_null());
}

TEST(primitive, null_value_by_nullopt) {
    primitive val(std::nullopt);
    EXPECT_EQ(val.get_type(), ignite_type::NIL);
    EXPECT_TRUE(val.is_null());
}
