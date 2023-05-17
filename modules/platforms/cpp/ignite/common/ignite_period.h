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
 * @brief A date-based amount of time.
 *
 * This is modeled after java.time.Period.
 */
class ignite_period {
public:
    /**
     * Default constructor.
     */
    constexpr ignite_period() noexcept = default;

    /**
     * Constructor.
     *
     * @param years The number of years.
     * @param months The number of months.
     * @param days The number of days.
     */
    constexpr ignite_period(std::int32_t years, std::int32_t months, std::int32_t days)
        : years(years)
        , months(months)
        , days(days) {
        // TODO: check that arguments are in valid ranges.
    }

    /**
     * Gets the years field.
     */
    constexpr std::int32_t get_years() const noexcept { return years; }

    /**
     * Gets the months field.
     */
    constexpr std::int32_t get_months() const noexcept { return months; }

    /**
     * Gets the days field.
     */
    constexpr std::int32_t get_days() const noexcept { return days; }

    /**
     * compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    constexpr int compare(const ignite_period &other) const noexcept {
        if (years != other.years) {
            return years - other.years;
        }
        if (months != other.months) {
            return months - other.months;
        }
        return days - other.days;
    }

private:
    std::int_least32_t years = 0;
    std::int_least32_t months = 0;
    std::int_least32_t days = 0;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
constexpr bool operator==(const ignite_period &lhs, const ignite_period &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const ignite_period &lhs, const ignite_period &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const ignite_period &lhs, const ignite_period &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const ignite_period &lhs, const ignite_period &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const ignite_period &lhs, const ignite_period &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const ignite_period &lhs, const ignite_period &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

} // namespace ignite
