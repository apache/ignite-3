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

#include "ignite/client/ignite_client_authenticator.h"

#include <string>

namespace ignite {

/**
 * Basic authenticator with username and password.
 *
 * Credentials are sent to the server in plain text, unless SSL/TLS is enabled.
 */
class basic_authenticator : public ignite_client_authenticator {
public:
    /** Type constant. */
    inline static const std::string TYPE{"basic"};

    // Default
    basic_authenticator() = default;

    /**
     * Constructor.
     *
     * @param username Username.
     * @param password Password.
     */
    basic_authenticator(std::string username, std::string password)
        : m_username(std::move(username))
        , m_password(std::move(password)) {}

    /**
     * Get authenticator type.
     *
     * @return Authenticator type.
     */
    [[nodiscard]] const std::string &get_type() const override { return TYPE; }

    /**
     * Get identity.
     *
     * @return Username.
     */
    [[nodiscard]] const std::string &get_identity() const override { return m_username; }

    /**
     * Set username.
     *
     * @param username Username.
     */
    void set_username(std::string username) { m_username = std::move(username); };

    /**
     * Get secret.
     *
     * @return Password.
     */
    [[nodiscard]] const std::string &get_secret() const override { return m_password; }

    /**
     * Set password.
     *
     * @param password Password.
     */
    void set_password(std::string password) { m_password = std::move(password); };

private:
    /** Username. */
    std::string m_username;

    /** Password. */
    std::string m_password;
};

} // namespace ignite
