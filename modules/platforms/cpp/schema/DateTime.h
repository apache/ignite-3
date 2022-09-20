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

#include "Date.h"
#include "Time.h"

#include <cstdint>

namespace ignite {

/**
 * @brief A date together with time of day with nanosecond precision.
 *
 * This is modeled after java.time.LocalDateTime.
 */
class DateTime : public Date, public Time {
public:
    /**
     * Default constructor.
     */
    constexpr DateTime() noexcept = default;

    /**
     * Constructor.
     *
     * @param date
     * @param time
     */
    constexpr DateTime(const Date &date, const Time &time)
        : Date(date)
        , Time(time) { }

    /**
     * Compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    constexpr int compare(const DateTime &other) const noexcept {
        const Date &date = *this;
        const Date &otherDate = other;
        if (int cmp = date.compare(otherDate)) {
            return cmp;
        }

        const Time &time = *this;
        const Time &otherTime = other;
        return time.compare(otherTime);
    }
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
constexpr bool operator==(const DateTime &lhs, const DateTime &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const DateTime &lhs, const DateTime &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const DateTime &lhs, const DateTime &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const DateTime &lhs, const DateTime &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const DateTime &lhs, const DateTime &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const DateTime &lhs, const DateTime &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

} // namespace ignite
