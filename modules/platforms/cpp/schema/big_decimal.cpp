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

#include "big_decimal.h"

#include <cstring>
#include <utility>

namespace ignite {

big_decimal::big_decimal(const int8_t *mag, int32_t len, int32_t scale, int32_t sign, bool bigEndian)
    : scale(scale & 0x7FFFFFFF)
    , magnitude(mag, len, sign, bigEndian) {
    // No-op.
}

big_decimal::big_decimal(const big_decimal &other)
    : scale(other.scale)
    , magnitude(other.magnitude) {
    // No-op.
}

big_decimal::big_decimal(int64_t val)
    : scale(0)
    , magnitude(val) {
    // No-op.
}

big_decimal::big_decimal(int64_t val, int32_t scale)
    : scale(scale)
    , magnitude(val) {
    // No-op.
}

big_decimal::big_decimal(const big_integer &val, int32_t scale)
    : scale(scale)
    , magnitude(val) {
    // No-op.
}

big_decimal::big_decimal(const char *val, int32_t len)
    : scale(0)
    , magnitude(0) {
    assign_string(val, len);
}

big_decimal &big_decimal::operator=(const big_decimal &other) {
    scale = other.scale;
    magnitude = other.magnitude;

    return *this;
}

big_decimal::operator double() const {
    return ToDouble();
}

big_decimal::operator int64_t() const {
    return to_int64();
}

double big_decimal::ToDouble() const {
    std::stringstream stream;
    stream << *this;

    double result;
    stream >> result;
    return result;
}

int64_t big_decimal::to_int64() const {
    if (scale == 0)
        return magnitude.to_int64();

    big_decimal zeroScaled;

    set_scale(0, zeroScaled);

    return zeroScaled.magnitude.to_int64();
}

void big_decimal::set_scale(int32_t newScale, big_decimal &res) const {
    if (scale == newScale)
        return;

    int32_t diff = scale - newScale;

    big_integer adjustment;

    if (diff > 0) {
        big_integer::get_power_of_ten(diff, adjustment);

        magnitude.divide(adjustment, res.magnitude);
    } else {
        big_integer::get_power_of_ten(-diff, adjustment);

        magnitude.multiply(adjustment, res.magnitude);
    }

    res.scale = newScale;
}

void big_decimal::swap(big_decimal &second) {
    using std::swap;

    swap(scale, second.scale);
    magnitude.swap(second.magnitude);
}

int32_t big_decimal::get_magnitude_length() const {
    return magnitude.mag.size();
}

void big_decimal::assign_string(const char *val, int32_t len) {
    std::stringstream converter;

    converter.write(val, len);

    converter >> *this;
}

void big_decimal::assign_int64(int64_t val) {
    magnitude.assign_int64(val);

    scale = 0;
}

void big_decimal::assign_double(double val) {
    std::stringstream converter;

    converter.precision(16);

    converter << val;
    converter >> *this;
}

void big_decimal::assign_uint64(uint64_t val) {
    magnitude.assign_uint64(val);

    scale = 0;
}

int big_decimal::compare(const big_decimal &other) const {
    if (is_zero() && other.is_zero())
        return 0;

    if (scale == other.scale)
        return magnitude.compare(other.magnitude);
    else if (scale > other.scale) {
        big_decimal scaled;

        other.set_scale(scale, scaled);

        return magnitude.compare(scaled.magnitude);
    } else {
        big_decimal scaled;

        set_scale(other.scale, scaled);

        return scaled.magnitude.compare(other.magnitude);
    }
}

} // namespace ignite
