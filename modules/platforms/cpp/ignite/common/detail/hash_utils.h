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

#include "murmur3_hash.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace ignite::detail {

/**
 * Adapter to original murmur3 hash function
 * @param key Pointer to begin of data.
 * @param len Data length
 * @param seed Seed.
 * @return Hash value.
 */
inline std::uint64_t murmur_original( const void * key, std::size_t len, std::uint64_t seed) {
    std::uint64_t res[2];
    MurmurHash3_x64_128(key, len, seed, res);

    return res[0];
}

/**
 * Calculates hash.
 * @param data Data.
 * @param seed Seed.
 * @return Hash value.
 */
template<typename T>
std::uint64_t hash64(T data, std::uint64_t seed) {
    return murmur_original(&data, sizeof(data), seed);
}

/**
 * Calculates hash.
 * @param data Pointer to begin of data.
 * @param len Data length.
 * @param seed Seed.
 * @return Hash value.
 */
inline std::uint64_t hash64(const void *data, size_t len, std::uint64_t seed) {
    return murmur_original(data, len, seed);
}

/**
 * Calculates hash.
 * @param data Data.
 * @param seed Seed.
 * @return Hash value.
 */
template<typename T>
std::int32_t hash32(T data, std::uint64_t seed = 0) {
    auto hash = hash64<T>(data, seed);

    return static_cast<std::int32_t>(hash ^ hash >> 32);
}

/**
 * Calculates hash.
 * @param data Pointer to begin of data.
 * @param len Data length.
 * @param seed Seed.
 * @return Hash value.
 */
inline std::int32_t hash32(const void *data, size_t len, std::uint64_t seed) {
    auto hash = hash64(data, len, seed);

    return static_cast<std::int32_t>(hash ^ hash >> 32);
}

/**
 * Calculation hash of @code bool value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(bool val);

/**
 * Calculation hash of @code int8_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::int8_t val);

/**
 * Calculation hash of @code uint8_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::uint8_t val);

/**
 * Calculation hash of @code int16_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::int16_t val);

/**
 * Calculation hash of @code uint16_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::uint16_t val);

/**
 * Calculation hash of @code int32_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::int32_t val);

/**
 * Calculation hash of @code uint32_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::uint32_t val);

/**
 * Calculation hash of @code int64_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::int64_t val);

/**
 * Calculation hash of @code uint64_t value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(std::uint64_t val);

/**
 * Calculation hash of @code float value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(float val);

/**
 * Calculation hash of @code double value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(double val);

/**
 * Calculation hash of @code ignite::big_decimal value.
 * @param val Value
 * @param scale Required scale.
 * @return Hash.
 */
std::int32_t hash(const big_decimal &val, std::int16_t scale);

/**
 * Calculation hash of @code ignite::uuid value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(const uuid &val);

/**
 * Calculation hash of @code ignite_date value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(const ignite_date &val);

/**
 * Calculation hash of @code ignite_date_time value.
 * @param val Value
 * @param precision Required precision.
 * @return Hash.
 */
std::int32_t hash(const ignite_date_time &val, std::int32_t precision);

/**
 * Calculation hash of @code ignite_time value.
 * @param val Value
 * @param precision Required precision.
 * @return Hash.
 */
std::int32_t hash(const ignite_time &val, std::int32_t precision);

/**
 * Calculation hash of @code ignite_timestamp value.
 * @param val Value
 * @param precision Required precision.
 * @return Hash.
 */
std::int32_t hash(const ignite_timestamp &val, std::int32_t precision);

/**
 * Calculation hash of @code std::string value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(const std::string &val);

/**
 * Calculation hash of @code std::vector<std::byte> value.
 * @param val Value
 * @return Hash.
 */
std::int32_t hash(const std::vector<std::byte> &val);

} // namespace ignite::detail