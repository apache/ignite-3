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
#include <string>

namespace ignite {

/**
 * Connection end point structure.
 */
struct end_point {
    // Default
    end_point() = default;

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     */
    end_point(std::string host, uint16_t port)
        : host(std::move(host))
        , port(port) {}

    /**
     * Convert to string.
     *
     * @return String form.
     */
    [[nodiscard]] std::string to_string() const { return host + ":" + std::to_string(port); }

    /**
     * compare to another instance.
     *
     * @param other Another instance.
     * @return Negative value if less, positive if larger and zero, if equals
     *   another instance.
     */
    [[nodiscard]] int compare(const end_point &other) const {
        if (port < other.port)
            return -1;

        if (port > other.port)
            return 1;

        return host.compare(other.host);
    }

    /** Remote host. */
    std::string host;

    /** TCP port. */
    uint16_t port{0};
};

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if equal.
 */
inline bool operator==(const end_point &val1, const end_point &val2) {
    return val1.port == val2.port && val1.host == val2.host;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if not equal.
 */
inline bool operator!=(const end_point &val1, const end_point &val2) {
    return !(val1 == val2);
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if less.
 */
inline bool operator<(const end_point &val1, const end_point &val2) {
    return val1.compare(val2) < 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if less or equal.
 */
inline bool operator<=(const end_point &val1, const end_point &val2) {
    return val1.compare(val2) <= 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if greater.
 */
inline bool operator>(const end_point &val1, const end_point &val2) {
    return val1.compare(val2) > 0;
}

/**
 * Comparison operator.
 *
 * @param val1 First value.
 * @param val2 Second value.
 * @return True if greater or equal.
 */
inline bool operator>=(const end_point &val1, const end_point &val2) {
    return val1.compare(val2) >= 0;
}

} // namespace ignite
