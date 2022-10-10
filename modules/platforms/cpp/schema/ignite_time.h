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
 * @brief A time of day with nanosecond precision.
 *
 * This is modeled after java.time.LocalTime.
 */
class ignite_time {
public:
    /**
     * Default constructor.
     */
    constexpr ignite_time() noexcept = default;

    /**
     * Constructor.
     *
     * @param hour Hour-of-day, from 0 to 23.
     * @param minute Minute-of-hour, from 0 to 59.
     * @param second Second-of-minute, from 0 to 59.
     * @param nano Nano-of-second, from 0 to 999,999,999.
     */
    constexpr ignite_time(
        std::int_fast8_t hour, std::int_fast8_t minute, std::int_fast8_t second = 0, std::int32_t nano = 0)
        : hour(hour)
        , minute(minute)
        , second(second)
        , nano(nano) {
        // TODO: check that arguments are in valid ranges.
    }

    /**
     * Gets the hour-of-day field.
     */
    constexpr auto get_hour() const noexcept { return hour; }

    /**
     * Gets the minute-of-hour field.
     */
    constexpr auto get_minute() const noexcept { return minute; }

    /**
     * Gets the second-of-minute field.
     */
    constexpr auto get_second() const noexcept { return second; }

    /**
     * Gets the nano-of-second field.
     */
    constexpr std::int32_t get_nano() const noexcept { return nano; }

    /**
     * Compare to another value.
     *
     * @param other Instance to compare to.
     * @return Zero if equals, negative number if less, and positive if greater.
     */
    constexpr int compare(const ignite_time &other) const noexcept {
        if (hour != other.hour) {
            return hour - other.hour;
        }
        if (minute != other.minute) {
            return minute - other.minute;
        }
        if (second != other.second) {
            return second - other.second;
        }
        return nano - other.nano;
    }

private:
    std::int8_t hour = 0;
    std::int8_t minute = 0;
    std::int8_t second = 0;
    std::int32_t nano = 0;
};

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is equal to the second.
 */
constexpr bool operator==(const ignite_time &lhs, const ignite_time &rhs) noexcept {
    return lhs.compare(rhs) == 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is not equal to the second.
 */
constexpr bool operator!=(const ignite_time &lhs, const ignite_time &rhs) noexcept {
    return lhs.compare(rhs) != 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than the second.
 */
constexpr bool operator<(const ignite_time &lhs, const ignite_time &rhs) noexcept {
    return lhs.compare(rhs) < 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is less than or equal to the second.
 */
constexpr bool operator<=(const ignite_time &lhs, const ignite_time &rhs) noexcept {
    return lhs.compare(rhs) <= 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than the second.
 */
constexpr bool operator>(const ignite_time &lhs, const ignite_time &rhs) noexcept {
    return lhs.compare(rhs) > 0;
}

/**
 * @brief Comparison operator.
 *
 * @param lhs First value.
 * @param rhs Second value.
 * @return true If the first value is greater than or equal to the second.
 */
constexpr bool operator>=(const ignite_time &lhs, const ignite_time &rhs) noexcept {
    return lhs.compare(rhs) >= 0;
}

} // namespace ignite
