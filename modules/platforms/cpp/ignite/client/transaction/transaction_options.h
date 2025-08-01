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

/**
 * Transaction options.
 */
class transaction_options {
public:
    transaction_options() = default;
    ~transaction_options() = default;
    transaction_options(const transaction_options&) = default;
    transaction_options(transaction_options &&) = default;
    transaction_options &operator=(const transaction_options &other) = default;
    transaction_options &operator=(transaction_options &&other) = default;

    [[nodiscard]] std::int64_t get_timeout_millis() const { return m_timeout_millis; }

    transaction_options& set_timeout_millis(std::int64_t timeout_millis) {
        m_timeout_millis = timeout_millis;
        return *this;
    }

    [[nodiscard]] bool is_read_only() const { return m_read_only; }

    transaction_options& set_read_only(bool read_only) {
        m_read_only = read_only;
        return *this;
    }
private:
    std::int64_t m_timeout_millis = 0;
    bool m_read_only = false;
};
