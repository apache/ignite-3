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

big_decimal::big_decimal(const int8_t *mag, int32_t len, int32_t scale, int8_t sign, bool bigEndian)
    : scale(scale & 0x7FFFFFFF)
    , magnitude(mag, len, sign, bigEndian) {
}

big_decimal::big_decimal(int64_t val)
    : scale(0)
    , magnitude(val) {
}

big_decimal::big_decimal(int64_t val, int32_t scale)
    : scale(scale)
    , magnitude(val) {
}

big_decimal::big_decimal(big_integer val, int32_t scale)
    : scale(scale)
    , magnitude(std::move(val)) {
    // No-op.
}

big_decimal::big_decimal(const char *val, int32_t len)
    : scale(0)
    , magnitude(0) {
    assign_string(val, len);
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
    return int32_t(magnitude.mag.size());
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

std::ostream &operator<<(std::ostream &os, const big_decimal &val) {
    const big_integer &unscaled = val.get_unscaled_value();

    // Zero magnitude case. Scale does not matter.
    if (unscaled.get_magnitude().empty())
        return os << '0';

    // Scale is zero or negative. No decimal point here.
    if (val.scale <= 0) {
        os << unscaled;

        // Adding zeroes if needed.
        for (int32_t i = 0; i < -val.scale; ++i)
            os << '0';

        return os;
    }

    // Getting magnitude as a string.
    std::stringstream converter;

    converter << unscaled;

    std::string magStr = converter.str();

    auto magLen = int32_t(magStr.size());

    int32_t magBegin = 0;

    // If value is negative passing minus sign.
    if (magStr[magBegin] == '-') {
        os << magStr[magBegin];

        ++magBegin;
        --magLen;
    }

    // Finding last non-zero char. There is no sense in trailing zeroes
    // beyond the decimal point.
    int32_t lastNonZero = static_cast<int32_t>(magStr.size()) - 1;

    while (lastNonZero >= magBegin && magStr[lastNonZero] == '0')
        --lastNonZero;

    // This is expected as we already covered zero number case.
    assert(lastNonZero >= magBegin);

    int32_t dotPos = magLen - val.scale;

    if (dotPos <= 0) {
        // Means we need to add leading zeroes.
        os << '0' << '.';

        while (dotPos < 0) {
            ++dotPos;

            os << '0';
        }

        os.write(&magStr[magBegin], lastNonZero - magBegin + 1);
    } else {
        // Decimal point is in the middle of the number.
        // Just output everything before the decimal point.
        os.write(&magStr[magBegin], dotPos);

        int32_t afterDot = lastNonZero - dotPos - magBegin + 1;

        if (afterDot > 0) {
            os << '.';

            os.write(&magStr[magBegin + dotPos], afterDot);
        }
    }

    return os;
}

std::istream &operator>>(std::istream &is, big_decimal &val) {
    std::istream::sentry sentry(is);

    // Return zero if input failed.
    val.assign_int64(0);

    if (!is)
        return is;

    // Current char.
    int c = is.peek();

    // Current value parts.
    uint64_t part = 0;
    int32_t partDigits = 0;
    int32_t scale = -1;
    int32_t sign = 1;

    big_integer &mag = val.magnitude;
    big_integer pow;
    big_integer bigPart;

    if (!is)
        return is;

    // Checking sign.
    if (c == '-' || c == '+') {
        if (c == '-')
            sign = -1;

        is.ignore();
        c = is.peek();
    }

    // Reading number itself.
    while (is) {
        if (isdigit(c)) {
            part = part * 10 + (c - '0');
            ++partDigits;
        } else if (c == '.' && scale < 0) {
            // We have found decimal point. Starting counting scale.
            scale = 0;
        } else
            break;

        is.ignore();
        c = is.peek();

        if (part >= 1000000000000000000U) {
            big_integer::get_power_of_ten(partDigits, pow);
            mag.multiply(pow, mag);

            mag.add(part);

            part = 0;
            partDigits = 0;
        }

        // Counting scale if the decimal point have been encountered.
        if (scale >= 0)
            ++scale;
    }

    // Adding last part of the number.
    if (partDigits) {
        big_integer::get_power_of_ten(partDigits, pow);

        mag.multiply(pow, mag);

        mag.add(part);
    }

    // Adjusting scale.
    if (scale < 0)
        scale = 0;
    else
        --scale;

    // Reading exponent.
    if (c == 'e' || c == 'E') {
        is.ignore();

        int32_t exp = 0;
        is >> exp;

        scale -= exp;
    }

    val.scale = scale;

    if (sign < 0)
        mag.negate();

    return is;
}

} // namespace ignite
