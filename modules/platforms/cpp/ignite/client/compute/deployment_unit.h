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
 * @brief Deployment unit identifier.
 */
class deployment_unit {

public:
    static const inline std::string LATEST_VERSION{"latest"};

    // Delete
    deployment_unit() = delete;

    /**
     * Constructor.
     *
     * @param name Unit name.
     * @param version Unit version. Defaults to @c LATEST_VERSION.
     */
    deployment_unit(std::string name, std::string version = LATEST_VERSION) // NOLINT(google-explicit-constructor)
        : m_name(std::move(name))
        , m_version(std::move(version)) {}

    /**
     * Get name.
     *
     * @return Unit name.
     */
    [[nodiscard]] const std::string &get_name() const { return m_name; }

    /**
     * Set name.
     *
     * @param name Unit name to set.
     */
    void set_name(const std::string &name) { m_name = name; }

    /**
     * Get version.
     *
     * @return Unit version.
     */
    [[nodiscard]] const std::string &get_version() const { return m_version; }

    /**
     * Set version.
     *
     * @param version Unit version to set.
     */
    void set_version(const std::string &version) { m_version = version; }

private:
    /** Name. */
    std::string m_name;

    /** Version. */
    std::string m_version;
};

} // namespace ignite
