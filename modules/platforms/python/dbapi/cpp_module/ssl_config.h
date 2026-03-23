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

/**
 * A simple SSL configuration.
 */
struct ssl_config {
    /**
     * Constructor.
     *
     * @param enabled Flag indicating whether SSL is enabled.
     * @param ssl_key SSL Keyfile.
     * @param ssl_cert SSL Certificate.
     * @param ssl_ca_cert SSL CA Certificate.
     */
    ssl_config(bool enabled, const char *ssl_key, const char *ssl_cert, const char *ssl_ca_cert)
        : m_enabled(enabled)
        , m_ssl_keyfile(ssl_key ? ssl_key : "")
        , m_ssl_certfile(ssl_cert ? ssl_cert : "")
        , m_ssl_ca_certfile(ssl_ca_cert ? ssl_ca_cert : "")
      {}

    /** Flag indicating whether SSL is enabled. */
    bool m_enabled{false};

    /** SSL Key. */
    const std::string m_ssl_keyfile;

    /** SSL Certificate. */
    const std::string m_ssl_certfile;

    /** SSL CA Certificate. */
    const std::string m_ssl_ca_certfile;
};
