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

#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/meta/table_meta.h"
#include "ignite/odbc/protocol_version.h"

#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"

#include <cstdint>
#include <string>

namespace ignite
{

enum class client_type
{
    ODBC = 2
};

enum class request_type
{
    HANDSHAKE = 1
};

/**
 * Handshake request.
 */
class handshake_request
{
public:
    /**
     * Constructor.
     *
     * @param config Configuration.
     */
    explicit handshake_request(const configuration& config)
        : m_config(config) {}

    /**
     * Write request using provided writer.
     *
     * @param writer Writer.
     */
    void write(protocol::writer &writer, const protocol_version&) const;

private:
    /** Configuration. */
    const configuration& m_config;
};

/**
 * General response.
 */
class response
{
public:
    // Default
    response() = default;
    virtual ~response() = default;

    /**
     * Read response using provided reader.
     *
     * @param reader Reader.
     * @param ver Protocol version.
     */
    void read(protocol::reader& reader, const protocol_version& ver);

    /**
     * Get request processing status.
     *
     * @return Status.
     */
    [[nodiscard]] response_status get_state() const
    {
        return m_status;
    }

    /**
     * Get resulting error.
     * @return Error.
     */
    [[nodiscard]] const std::string& get_error() const
    {
        return m_error;
    }

protected:
    /**
     * Read data if response status is response_status::SUCCESS.
     */
    virtual void read_on_success(protocol::reader&, const protocol_version&) { }

private:
    /** Request processing status. */
    response_status m_status{response_status::UNKNOWN_ERROR};

    /** Error message. */
    std::string m_error;
};

/**
 * Handshake response.
 */
class handshake_response
{
public:
    // Default
    handshake_response() = default;

    /**
     * Check if the handshake has been accepted.
     * @return True if the handshake has been accepted.
     */
    [[nodiscard]] bool is_accepted() const
    {
        return m_accepted;
    }

    /**
     * Get optional error.
     * @return Optional error message.
     */
    [[nodiscard]] const std::string& get_error() const
    {
        return m_error;
    }

    /**
     * Current host Apache Ignite version.
     * @return Current host Apache Ignite version.
     */
    [[nodiscard]] const protocol_version& get_current_ver() const
    {
        return m_current_ver;
    }

    /**
     * Read response using provided reader.
     * @param reader Reader.
     */
    void read(protocol::reader &reader, const protocol_version&);

private:
    /** Handshake accepted. */
    bool m_accepted{false};

    /** Node's protocol version. */
    protocol_version m_current_ver;

    /** Optional error message. */
    std::string m_error;
};

} // namespace ignite
