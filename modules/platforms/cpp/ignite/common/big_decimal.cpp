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
#include "bytes.h"

#include <cstring>

namespace ignite {

std::size_t big_decimal::byte_size() const noexcept {
    return sizeof(m_scale) + get_unscaled_value().byte_size();
}

void big_decimal::store_bytes(std::byte *data) const {
    bytes::store<endian::LITTLE, std::uint16_t>(data, m_scale);
    get_unscaled_value().store_bytes(data + sizeof(m_scale));
}

big_decimal::big_decimal(const std::byte *data, std::size_t size) {
    if (size < sizeof(m_scale)) {
        return;
    }

    m_scale = bytes::load<endian::LITTLE, std::int16_t>(data);
    m_magnitude = big_integer(data + sizeof(m_scale), size - sizeof(m_scale));
}

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
    if (unscaled.is_zero())
        return os << '0';

    std::string serialized = unscaled.to_string();

    auto sign = unscaled.get_sign();
    std::size_t adj = sign > 0 ? 0 : 1;

    if (val.m_scale < 0) {
        std::string zeros(-val.m_scale, '0');
        serialized += zeros;
    } else if (val.m_scale > 0) {
        if (static_cast<std::size_t>(val.m_scale) >= serialized.length() - adj) {
            std::string zeros(val.m_scale - serialized.length() + adj + 1, '0');
            serialized.insert(adj, zeros);
        }
        serialized.insert(serialized.end() - val.m_scale, '.');
    }

    os << serialized;
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

    if (!is)
        return is;

    bool scale_found = false;
    std::int16_t scale = 0;

    std::string str;
    while (is && (isdigit(c) || c == '-' || c == '+' || c == '.')) {
        if (scale_found) {
            scale++;
        }

        if (c == '.') {
            scale_found = true;
        } else {
            str.push_back(c);
        }
        is.ignore();
        c = is.peek();
    }

    // Reading exponent.
    if (c == 'e' || c == 'E') {
        is.ignore();

        int32_t exp = 0;
        is >> exp;

        scale -= exp;
    }

    val.m_magnitude.assign_string(str.c_str());
    val.m_scale = scale;

    return is;
}

void big_decimal::add(const big_decimal &other, big_decimal &res) const {
    big_decimal v1 = *this;
    big_decimal v2 = other;

    auto scale = std::max(m_scale, other.m_scale);

    v1.set_scale(scale, v1);
    v2.set_scale(scale, v2);

    res.m_magnitude = v1.m_magnitude + v2.m_magnitude;
    res.m_scale = scale;
}

void big_decimal::subtract(const big_decimal &other, big_decimal &res) const {
    big_decimal v1 = *this;
    big_decimal v2 = other;

    res.m_scale = std::max(m_scale, other.m_scale);

    v1.set_scale(res.m_scale, v1);
    v2.set_scale(res.m_scale, v2);

    res.m_magnitude = v1.m_magnitude - v2.m_magnitude;
}

void big_decimal::multiply(const big_decimal &other, big_decimal &res) const {
    res.m_magnitude = m_magnitude * other.m_magnitude;
    res.m_scale = m_scale + other.m_scale;
}

// Let p1, s1 be the precision and scale of the first operand
// Let p2, s2 be the precision and scale of the second operand
// Let p, s be the precision and scale of the result
// Let d be the number of whole digits in the result
// Then the result type is a decimal with:
//     d = p1 - s1 + s2
//     s < max(6, s1 + p2 + 1)
//     p = d + s
//     p and s are capped at their maximum values

void big_decimal::divide(const big_decimal &other, big_decimal &res) const {
    big_decimal v1 = *this;
    big_decimal v2 = other;

    std::int16_t scale = std::max(6, get_scale() + other.get_precision() + 1);

    v1.set_scale(scale, v1);

    res.m_magnitude = v1.m_magnitude / v2.m_magnitude;
    res.m_scale = scale - other.get_scale();
    res.set_scale(scale, res);
}

} // namespace ignite
