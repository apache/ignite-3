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

#pragma once
#include "big_decimal.h"
#include "ignite_date_time.h"
#include "ignite_time.h"
#include "ignite_timestamp.h"
#include "uuid.h"

#include "Murmur3Hash.h"

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace ignite::detail {
constexpr std::uint64_t C1 = 0x87'C3'7B'91'11'42'53'D5L;
constexpr std::uint64_t C2 = 0x4C'F5'AD'43'27'45'93'7FL;
constexpr int R1 = 31;
constexpr int R2 = 27;
constexpr int R3 = 33;
constexpr int M = 5;
constexpr int N1 = 0x52dce729;
constexpr int N2 = 0x38495ab5;

inline std::uint64_t murmur_original( const void * key, const int len, std::uint64_t seed) {
    std::uint64_t res[2];
    MurmurHash3_x64_128(key, len, seed, res);

    return res[0];
}

inline std::uint64_t hash64(std::int8_t data, std::uint64_t seed) {

    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::uint8_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::int16_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::uint16_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::int32_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::uint32_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::int64_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(std::uint64_t data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

inline std::uint64_t hash64(const std::uint8_t *data, size_t off, size_t len, std::uint64_t seed) {
    return murmur_original(data + off, len, seed);
}

template<typename T>
std::int32_t hash32(T data, std::uint64_t seed = 0) {
    auto hash = hash64(data, seed);

    return static_cast<std::int32_t>(hash ^ hash >> 32);
}

inline std::int32_t hash32(const std::uint8_t *data, size_t off, size_t len, std::uint64_t seed) {
    auto hash = hash64(data, off, len, seed);

    return static_cast<std::int32_t>(hash ^ hash >> 32);
}

std::int32_t hash(bool val);
std::int32_t hash(std::int8_t val);
std::int32_t hash(std::uint8_t val);
std::int32_t hash(std::int16_t val);
std::int32_t hash(std::uint16_t val);
std::int32_t hash(std::int32_t val);
std::int32_t hash(std::uint32_t val);
std::int32_t hash(std::int64_t val);
std::int32_t hash(std::uint64_t val);
std::int32_t hash(float val);
std::int32_t hash(double val);
std::int32_t hash(const big_decimal &val, std::int16_t scale);
std::int32_t hash(const uuid &val);
std::int32_t hash(const ignite_date &val);
std::int32_t hash(const ignite_date_time &val, std::int32_t precision);
std::int32_t hash(const ignite_time &val, std::int32_t precision);
std::int32_t hash(const ignite_timestamp &val, std::int32_t precision);
std::int32_t hash(const std::string &val);
std::int32_t hash(const std::vector<std::byte> &val);

} // namespace ignite::detail