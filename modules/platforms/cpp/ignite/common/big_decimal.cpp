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

namespace ignite {

void big_decimal::set_scale(std::int16_t new_scale, big_decimal &res) const {
    if (m_scale == new_scale)
        return;

    auto diff = std::int16_t(m_scale - new_scale);

    big_integer adjustment;

    if (diff > 0) {
        big_integer::get_power_of_ten(diff, adjustment);

        m_magnitude.divide(adjustment, res.m_magnitude);
    } else {
        big_integer::get_power_of_ten(-diff, adjustment);

        m_magnitude.multiply(adjustment, res.m_magnitude);
    }

    res.m_scale = new_scale;
}

int big_decimal::compare(const big_decimal &other) const {
    if (is_zero() && other.is_zero())
        return 0;

    if (m_scale == other.m_scale)
        return m_magnitude.compare(other.m_magnitude);
    else if (m_scale > other.m_scale) {
        big_decimal scaled;

        other.set_scale(m_scale, scaled);

        return m_magnitude.compare(scaled.m_magnitude);
    } else {
        big_decimal scaled;

        set_scale(other.m_scale, scaled);

        return scaled.m_magnitude.compare(other.m_magnitude);
    }
}

std::ostream &operator<<(std::ostream &os, const big_decimal &val) {
    const big_integer &unscaled = val.get_unscaled_value();

    // Zero magnitude case. Scale does not matter.
    if (unscaled.get_magnitude().empty())
        return os << '0';

    // Scale is zero or negative. No decimal point here.
    if (val.m_scale <= 0) {
        os << unscaled;

        // Adding zeroes if needed.
        for (int32_t i = 0; i < -val.m_scale; ++i)
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

    int32_t dotPos = magLen - val.m_scale;

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

    big_integer &mag = val.m_magnitude;
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

    val.m_scale = scale;

    if (sign < 0)
        mag.negate();

    return is;
}
} // namespace ignite
