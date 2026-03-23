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

#include <string>

namespace ignite {

/**
 * @brief Ignite client authenticator.
 *
 * Provides authentication information during handshake.
 */
class ignite_client_authenticator {
public:
    /**
     * Get authenticator type.
     *
     * @return Authenticator type.
     */
    [[nodiscard]] virtual const std::string &get_type() const = 0;

    /**
     * Get identity.
     *
     * @return Identity.
     */
    [[nodiscard]] virtual const std::string &get_identity() const = 0;

    /**
     * Get secret.
     *
     * @return Secret.
     */
    [[nodiscard]] virtual const std::string &get_secret() const = 0;

protected:
    // Default
    ignite_client_authenticator() = default;
    virtual ~ignite_client_authenticator() = default;
};

} // namespace ignite
