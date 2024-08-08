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

#include <ignite/common/big_decimal.h>
#include <ignite/common/bit_array.h>
#include <ignite/common/ignite_date.h>
#include <ignite/common/ignite_date_time.h>
#include <ignite/common/ignite_time.h>
#include <ignite/common/ignite_timestamp.h>
#include <ignite/common/uuid.h>

#include <cstdint>
#include <string>

/**
 * All columns type table mapping (@see ignite_runner_suite::TABLE_NAME_ALL_COLUMNS).
 */
struct all_fields_type {
    all_fields_type() = default;

    explicit all_fields_type(std::int64_t key)
        : m_key(key) {}

    std::int64_t m_key{0};
    std::string m_str;
    std::int8_t m_int8{0};
    std::int16_t m_int16{0};
    std::int32_t m_int32{0};
    std::int64_t m_int64{0};
    float m_float{.0f};
    double m_double{.0};
    ignite::uuid m_uuid;
    ignite::ignite_date m_date;
    ignite::ignite_time m_time;
    ignite::ignite_time m_time2;
    ignite::ignite_date_time m_datetime;
    ignite::ignite_date_time m_datetime2;
    ignite::ignite_timestamp m_timestamp;
    ignite::ignite_timestamp m_timestamp2;
    std::vector<std::byte> m_blob;
    ignite::big_decimal m_decimal;
    bool m_boolean{false};
};

namespace ignite {

template<>
inline ignite_tuple convert_to_tuple(all_fields_type &&value) {
    ignite_tuple tuple;

    tuple.set("key", value.m_key);
    tuple.set("str", value.m_str);
    tuple.set("int8", value.m_int8);
    tuple.set("int16", value.m_int16);
    tuple.set("int32", value.m_int32);
    tuple.set("int64", value.m_int64);
    tuple.set("float", value.m_float);
    tuple.set("double", value.m_double);
    tuple.set("uuid", value.m_uuid);
    tuple.set("date", value.m_date);
    tuple.set("time", value.m_time);
    tuple.set("time2", value.m_time2);
    tuple.set("datetime", value.m_datetime);
    tuple.set("datetime2", value.m_datetime2);
    tuple.set("timestamp", value.m_timestamp);
    tuple.set("timestamp2", value.m_timestamp2);
    tuple.set("blob", value.m_blob);
    tuple.set("decimal", value.m_decimal);
    tuple.set("boolean", value.m_boolean);

    return tuple;
}

template<>
inline all_fields_type convert_from_tuple(ignite_tuple &&value) {
    all_fields_type res;

    res.m_key = value.get<std::int64_t>("key");

    if (value.column_count() > 1) {
        res.m_str = value.get<std::string>("str");
        res.m_int8 = value.get<std::int8_t>("int8");
        res.m_int16 = value.get<std::int16_t>("int16");
        res.m_int32 = value.get<std::int32_t>("int32");
        res.m_int64 = value.get<std::int64_t>("int64");
        res.m_float = value.get<float>("float");
        res.m_double = value.get<double>("double");
        res.m_uuid = value.get<uuid>("uuid");
        res.m_date = value.get<ignite_date>("date");
        res.m_time = value.get<ignite_time>("time");
        res.m_time2 = value.get<ignite_time>("time2");
        res.m_datetime = value.get<ignite_date_time>("datetime");
        res.m_datetime2 = value.get<ignite_date_time>("datetime2");
        res.m_timestamp = value.get<ignite_timestamp>("timestamp");
        res.m_timestamp2 = value.get<ignite_timestamp>("timestamp2");
        res.m_blob = value.get<std::vector<std::byte>>("blob");
        res.m_decimal = value.get<big_decimal>("decimal");
        res.m_boolean = value.get<bool>("boolean");
    }

    return res;
}

} // namespace ignite
