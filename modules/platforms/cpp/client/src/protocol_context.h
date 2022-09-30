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

#include "protocol_version.h"

namespace ignite::detail {

/**
 * Represents connection to the cluster.
 *
 * Considered established while there is connection to at least one server.
 */
class ProtocolContext {
public:
    /** The latest currently supported version. */
    static constexpr ProtocolVersion CURRENT_VERSION{3, 0, 0};

    /**
     * Get protocol version.
     *
     * @return protocol version.
     */
    [[nodiscard]] ProtocolVersion getVersion() const { return m_version; }

    /**
     * Set version.
     *
     * @param ver Version to set.
     */
    void setVersion(ProtocolVersion ver) { m_version = ver; }

private:
    /** Protocol version. */
    ProtocolVersion m_version{CURRENT_VERSION};
};

} // namespace ignite::detail
