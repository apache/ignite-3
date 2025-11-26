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

package org.apache.ignite.client.handler.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.network.configuration.SslConfigurationSchema;
import org.apache.ignite.internal.network.configuration.SslConfigurationValidator;

/**
 * Configuration schema for thin client connector.
 */
@Config
public class ClientConnectorConfigurationSchema {
    /** TCP port. */
    @Range(min = 1024, max = 0xFFFF)
    @Value(hasDefault = true)
    public final int port = 10800;

    /** Addresses (IPs or hostnames) to listen on. Will listen on all interfaces if empty. */
    @Value(hasDefault = true)
    public String[] listenAddresses = new String[0];

    /** Connect timeout. */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "connectTimeout")
    public final int connectTimeoutMillis = 5000;

    /** Idle timeout. */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "idleTimeout")
    public final long idleTimeoutMillis = 0;

    /** Server exception stack trace visibility. */
    @Value(hasDefault = true)
    public final boolean sendServerExceptionStackTraceToClient = false;

    /** SSL configuration schema. */
    @ConfigValue
    @SslConfigurationValidator
    public SslConfigurationSchema ssl;

    /** Metrics. */
    @Value(hasDefault = true)
    public final boolean metricsEnabled = true;
}
