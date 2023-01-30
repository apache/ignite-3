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

#include <cstdint>

namespace ignite {

/**
 * @brief A date.
 *
 * This is modeled after java.time.LocalDate.
 */
class ignite_date {
public:
    /**
     * Default constructor.
     */
    constexpr ignite_date() noexcept = default;

    /**
     * Constructor.
     *
     * @param year Year to represent, from -999,999,999 to 999,999,999.
     * @param month Month-of-year, from 1 (January) to 12 (December).
     * @param day_of_month Day-of-month, from 1 to 31.
     */
    constexpr ignite_date(int year, int month, int day_of_month)
        : year(year)
        , month(month)
        , day(day_of_month) {
        // TODO: check that arguments are in valid ranges.
    }

    /**
     * Gets the year field.
     */
    constexpr int get_year() const noexcept { return year; }

    /**
     * Gets the month-of-year field.
     */
    constexpr int get_month() const noexcept { return month; }

    /**
     * Gets the day-of-month field.
     */
    constexpr int get_day_of_month() const noexcept { return day; }

    /**
     * Compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    constexpr int compare(const ignite_date &other) const noexcept {
        if (year != other.year) {
            return year - other.year;
        }
        if (month != other.month) {
            return month - other.month;
        }
        return day - other.day;
    }

private:
    std::int32_t year = 0;
    std::int8_t month = 1;
    std::int8_t day = 1;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
constexpr bool operator==(const ignite_date &lhs, const ignite_date &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const ignite_date &lhs, const ignite_date &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const ignite_date &lhs, const ignite_date &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const ignite_date &lhs, const ignite_date &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const ignite_date &lhs, const ignite_date &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const ignite_date &lhs, const ignite_date &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

} // namespace ignite
