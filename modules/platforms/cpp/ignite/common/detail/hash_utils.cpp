// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//

#include "hash_utils.h"

namespace ignite::detail {

std::int32_t hash(bool val) {
    return hash32<std::uint8_t>(val ? 1 : 0);
}

std::int32_t hash(std::int8_t val) {
    return hash32(val);
}

std::int32_t hash(std::uint8_t val) {
    return hash32(val);
}

std::int32_t hash(std::int16_t val) {
    return hash32(val);
}

std::int32_t hash(std::uint16_t val) {
    return hash32(val);
}

std::int32_t hash(std::int32_t val) {
    return hash32(val);
}

std::int32_t hash(std::uint32_t val) {
    return hash32(val);
}

std::int32_t hash(std::int64_t val) {
    return hash32(val);
}

std::int32_t hash(std::uint64_t val) {
    return hash32(val);
}

std::int32_t hash(float val) {
    std::uint32_t v;

    static_assert(sizeof(v) == sizeof(val));

    std::memcpy(&v, &val, sizeof(val));

    return hash32(v);
}

std::int32_t hash(double val) {
    std::uint64_t v;

    static_assert(sizeof(v) == sizeof(val));

    std::memcpy(&v, &val, sizeof(val));

    return hash32(v);
}

std::int32_t hash(const big_decimal &val, std::int16_t scale) {
    big_decimal copy;
    val.set_scale(scale, copy, big_decimal::rounding_mode::HALF_UP);

    auto bytes = copy.get_unscaled_value().to_bytes();

    return hash32(reinterpret_cast<const uint8_t*>(bytes.data()), 0, bytes.size(), 0);
}

std::int32_t hash(const uuid &val) {
    return hash32(val.get_least_significant_bits(), hash32(val.get_most_significant_bits()));
}

std::int32_t hash(const ignite_date &val) {
    auto h_year = hash32(val.get_year());
    auto h_month = hash32<uint32_t>(val.get_month(), h_year);
    return hash32<uint32_t>(val.get_day_of_month(), h_month);
}

std::int32_t hash(const ignite_date_time &val, std::int32_t precision) {
    auto h_date = hash(val.date());
    auto h_time = hash(val.time(), precision);

    return hash32(h_date, h_time);
}

std::int32_t hash(const ignite_time &val, std::int32_t precision) {
    auto h_hour = hash32<std::uint32_t>(val.get_hour());
    auto h_min = hash32<std::uint32_t>(val.get_minute(), h_hour);
    auto h_sec = hash32<std::uint32_t>(val.get_second(), h_min);

    auto norm_nanos = normalize_nanos(val.get_nano(), precision);

    return hash32<std::uint32_t>(norm_nanos, h_sec);
}

std::int32_t hash(const ignite_timestamp &val, std::int32_t precision) {
    auto norm_nanos = normalize_nanos(val.get_nano(), precision);
    auto h_epoch = hash32(val.get_epoch_second());

    return hash32(norm_nanos, h_epoch);
}

std::int32_t hash(const std::string &val) {
    return hash32(reinterpret_cast<const std::uint8_t*>(val.data()), 0, val.length(), 0);
}

std::int32_t hash(const std::vector<std::byte> &val) {
    return hash32(reinterpret_cast<const std::uint8_t *>(val.data()), 0, val.size(), 0);
}
} // namespace ignite::detail