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

#include "ignite_date.h"
#include "ignite_time.h"

#include <cstdint>

namespace ignite {

/**
 * @brief A date together with time of day with nanosecond precision.
 *
 * This is modeled after java.time.LocalDateTime.
 */
class ignite_date_time : public ignite_date, public ignite_time {
public:
    /**
     * Default constructor.
     */
    constexpr ignite_date_time() noexcept = default;

    /**
     * Constructor.
     *
     * @param date
     * @param time
     */
    constexpr ignite_date_time(const ignite_date &date, const ignite_time &time)
        : ignite_date(date)
        , ignite_time(time) {}

    /**
     * Gets the date part of this date-time.
     */
    [[nodiscard]] constexpr const ignite_date &date() const noexcept { return *this; }

    /**
     * Gets the time part of this date-time.
     */
    [[nodiscard]] constexpr const ignite_time &time() const noexcept { return *this; }

    /**
     * compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    [[nodiscard]] constexpr int compare(const ignite_date_time &other) const noexcept {
        if (int cmp = date().compare(other.date())) {
            return cmp;
        }
        return time().compare(other.time());
    }
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
constexpr bool operator==(const ignite_date_time &lhs, const ignite_date_time &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const ignite_date_time &lhs, const ignite_date_time &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const ignite_date_time &lhs, const ignite_date_time &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const ignite_date_time &lhs, const ignite_date_time &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const ignite_date_time &lhs, const ignite_date_time &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const ignite_date_time &lhs, const ignite_date_time &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

} // namespace ignite
