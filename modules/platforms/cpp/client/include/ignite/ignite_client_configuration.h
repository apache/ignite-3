/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

#include <initializer_list>
#include <string>
#include <vector>

#include <ignite/ignite_logger.h>

namespace ignite
{

/**
 * Ignite client configuration.
 */
class IgniteClientConfiguration
{
public:
    /**
     * TCP port used by client by default if not specified explicitly.
     */
    static constexpr uint16_t DEFAULT_PORT = 10800;

    // Default
    IgniteClientConfiguration() = default;
    ~IgniteClientConfiguration() = default;
    IgniteClientConfiguration(IgniteClientConfiguration&&) = default;
    IgniteClientConfiguration(const IgniteClientConfiguration&) = default;
    IgniteClientConfiguration& operator=(IgniteClientConfiguration&&) = default;
    IgniteClientConfiguration& operator=(const IgniteClientConfiguration&) = default;

    /**
     * Constructor.
     *
     * @param endpoint Endpoints list.
     */
    IgniteClientConfiguration(std::initializer_list<std::string> endpoints) :
        m_endpoints(endpoints),
        m_logger() { }

    /**
     * Get endpoints.
     *
     * @see setEndpoints() for more detailed description.
     *
     * @return Endpoints.
     */
    [[nodiscard]]
    const std::vector<std::string>& getEndpoints() const
    {
        return m_endpoints;
    }

    /**
     * Set endpoints.
     *
     * Examples of supported formats:
     * - 192.168.1.25 - Default port is used, see DEFAULT_PORT;
     * - 192.168.1.25:780 - Custom port;
     * - 192.168.1.25:780..787  - Custom port range - ports are checked from
     *      lesser to greater until an open port is found;
     * - my-host.com - Default port is used, see DEFAULT_PORT;
     * - my-host.com:780 - Custom port;
     * - my-host.com:780..787 - Custom port range.
     *
     * @param endpoints Endpoints.
     */
    void setEndpoints(std::initializer_list<std::string> endpoints)
    {
        IgniteClientConfiguration::m_endpoints = endpoints;
    }

    /**
     * Get logger.
     *
     * @return Current logger.
     */
    [[nodiscard]]
    std::shared_ptr<IgniteLogger> getLogger() const
    {
        return m_logger;
    }

    /**
     * Set logger to be used by client.
     *
     * The logger is @c nullptr by default, which means no logging is performed.
     *
     * @param logger Logger to use.
     */
    void setLogger(std::shared_ptr<IgniteLogger> logger)
    {
        m_logger = std::move(logger);
    }

private:
    /** Endpoints. */
    std::vector<std::string> m_endpoints;

    /** Logger. */
    std::shared_ptr<IgniteLogger> m_logger;
};

} // namespace ignite