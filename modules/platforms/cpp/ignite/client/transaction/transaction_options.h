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

namespace ignite {
/**
 * @brief Transaction options.
 */
class transaction_options {
public:
    /**
     * Transaction timeout.
     *
     * @return Transaction timeout in milliseconds.
     */
    [[nodiscard]] std::int64_t get_timeout_millis() const { return m_timeout_millis; }

    /**
     * Sets new value for transaction timeout.
     *
     * @param timeout_millis Transaction timeout in milliseconds.
     * @return Reference to this for chaining.
     */
    transaction_options &set_timeout_millis(std::int64_t timeout_millis) {
        m_timeout_millis = timeout_millis;
        return *this;
    }

    /**
     * Transaction allow only read operations.
     *
     * @return True if only read operation are allowed false otherwise.
     */
    [[nodiscard]] bool is_read_only() const { return m_read_only; }

    /**
     * Change transaction to be read-only or read-write.
     *
     * @param read_only True if transaction should read-only, false if read-write.
     * @return Reference to this for chaining.
     */
    transaction_options &set_read_only(bool read_only) {
        m_read_only = read_only;
        return *this;
    }

private:
    std::int64_t m_timeout_millis = 0;
    bool m_read_only = false;
};
} // namespace ignite