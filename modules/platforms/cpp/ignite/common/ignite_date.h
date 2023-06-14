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
    constexpr ignite_date(std::int32_t year, std::int32_t month, std::int32_t day_of_month)
        : m_year(year)
        , m_month(std::int8_t(month))
        , m_day(std::int8_t(day_of_month)) {
        // TODO: check that arguments are in valid ranges.
    }

    /**
     * Gets the year field.
     */
    [[nodiscard]] constexpr std::int32_t get_year() const noexcept { return m_year; }

    /**
     * Gets the month-of-year field.
     */
    [[nodiscard]] constexpr std::int_fast8_t get_month() const noexcept { return m_month; }

    /**
     * Gets the day-of-month field.
     */
    [[nodiscard]] constexpr std::int_fast8_t get_day_of_month() const noexcept { return m_day; }

    /**
     * compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    [[nodiscard]] constexpr int compare(const ignite_date &other) const noexcept {
        if (m_year != other.m_year) {
            return m_year - other.m_year;
        }
        if (m_month != other.m_month) {
            return m_month - other.m_month;
        }
        return m_day - other.m_day;
    }

private:
    std::int_least32_t m_year = 0;
    std::int_least8_t m_month = 1;
    std::int_least8_t m_day = 1;
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
