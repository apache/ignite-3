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
 * Basic authenticator.
 */
class basic_authenticator : public ignite_client_authenticator {
public:
    /** Type constant. */
    inline static const std::string TYPE{"basic"};

    // Default
    basic_authenticator() = default;

    /**
     * Get authenticator type.
     *
     * @return Authenticator type.
     */
    const std::string& get_type() const override {
        return TYPE;
    }

    /**
     * Get identity.
     *
     * @return Identity.
     */
    [[nodiscard]] const std::string& get_identity() const override {
        return m_identity;
    }

    /**
     * Set identity.
     *
     * @param identity Identity.
     */
    void set_identity(std::string identity) {
        m_identity = std::move(identity);
    };

    /**
     * Get secret.
     *
     * @return Secret.
     */
    [[nodiscard]] const std::string& get_secret() const override {
        return m_secret;
    }

    /**
     * Set secret.
     *
     * @param secret Secret.
     */
    void set_secret(std::string secret) {
        m_secret = std::move(secret);
    };

private:
    /** Identity. */
    std::string m_identity;

    /** Secret. */
    std::string m_secret;
};

} // namespace ignite
