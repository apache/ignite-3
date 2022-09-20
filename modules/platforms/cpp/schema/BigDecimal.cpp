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

#include "BigDecimal.h"

#include <cstring>
#include <utility>

namespace ignite {
BigDecimal::BigDecimal()
    : scale(0)
    , magnitude(0) {
    // No-op.
}

BigDecimal::BigDecimal(const int8_t *mag, int32_t len, int32_t scale, int32_t sign, bool bigEndian)
    : scale(scale & 0x7FFFFFFF)
    , magnitude(mag, len, sign, bigEndian) {
    // No-op.
}

BigDecimal::BigDecimal(const BigDecimal &other)
    : scale(other.scale)
    , magnitude(other.magnitude) {
    // No-op.
}

BigDecimal::BigDecimal(int64_t val)
    : scale(0)
    , magnitude(val) {
    // No-op.
}

BigDecimal::BigDecimal(int64_t val, int32_t scale)
    : scale(scale)
    , magnitude(val) {
    // No-op.
}

BigDecimal::BigDecimal(const BigInteger &val, int32_t scale)
    : scale(scale)
    , magnitude(val) {
    // No-op.
}

BigDecimal::BigDecimal(const char *val, int32_t len)
    : scale(0)
    , magnitude(0) {
    AssignString(val, len);
}

BigDecimal::~BigDecimal() {
    // No-op.
}

BigDecimal &BigDecimal::operator=(const BigDecimal &other) {
    scale = other.scale;
    magnitude = other.magnitude;

    return *this;
}

BigDecimal::operator double() const {
    return ToDouble();
}

BigDecimal::operator int64_t() const {
    return ToInt64();
}

double BigDecimal::ToDouble() const {
    std::stringstream stream;
    stream << *this;

    double result;
    stream >> result;
    return result;
}

int64_t BigDecimal::ToInt64() const {
    if (scale == 0)
        return magnitude.ToInt64();

    BigDecimal zeroScaled;

    SetScale(0, zeroScaled);

    return zeroScaled.magnitude.ToInt64();
}

int32_t BigDecimal::GetScale() const {
    return scale;
}

void BigDecimal::SetScale(int32_t newScale, BigDecimal &res) const {
    if (scale == newScale)
        return;

    int32_t diff = scale - newScale;

    BigInteger adjustment;

    if (diff > 0) {
        BigInteger::GetPowerOfTen(diff, adjustment);

        magnitude.Divide(adjustment, res.magnitude);
    } else {
        BigInteger::GetPowerOfTen(-diff, adjustment);

        magnitude.Multiply(adjustment, res.magnitude);
    }

    res.scale = newScale;
}

int32_t BigDecimal::GetPrecision() const {
    return magnitude.GetPrecision();
}

const BigInteger &BigDecimal::GetUnscaledValue() const {
    return magnitude;
}

void BigDecimal::Swap(BigDecimal &second) {
    using std::swap;

    swap(scale, second.scale);
    magnitude.Swap(second.magnitude);
}

int32_t BigDecimal::GetMagnitudeLength() const {
    return magnitude.mag.size();
}

void BigDecimal::AssignString(const char *val, int32_t len) {
    std::stringstream converter;

    converter.write(val, len);

    converter >> *this;
}

void BigDecimal::AssignInt64(int64_t val) {
    magnitude.AssignInt64(val);

    scale = 0;
}

void BigDecimal::AssignDouble(double val) {
    std::stringstream converter;

    converter.precision(16);

    converter << val;
    converter >> *this;
}

void BigDecimal::AssignUint64(uint64_t val) {
    magnitude.AssignUint64(val);

    scale = 0;
}

int BigDecimal::compare(const BigDecimal &other) const {
    if (IsZero() && other.IsZero())
        return 0;

    if (scale == other.scale)
        return magnitude.compare(other.magnitude);
    else if (scale > other.scale) {
        BigDecimal scaled;

        other.SetScale(scale, scaled);

        return magnitude.compare(scaled.magnitude);
    } else {
        BigDecimal scaled;

        SetScale(other.scale, scaled);

        return scaled.magnitude.compare(other.magnitude);
    }
}

bool BigDecimal::IsNegative() const {
    return magnitude.IsNegative();
}

bool BigDecimal::IsZero() const {
    return magnitude.IsZero();
}

bool BigDecimal::IsPositive() const {
    return magnitude.IsPositive();
}

} // namespace ignite
