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

package org.apache.ignite.internal.network.configuration;

import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;

/** SSL configuration schema. */
@AbstractConfiguration
public class AbstractSslConfigurationSchema {
    /** Enable/disable SSL. */
    @Value(hasDefault = true)
    public final boolean enabled = false;

    /**
     * SSL client authentication. Set to "none" by default.
     *
     * <p>If set to "optional", the server will request a certificate from the client,
     * but will not fail if the client does not provide one.
     *
     * <p>If set to "require", the server will request a certificate from the client,
     * and will fail if the client does not provide one.
     */
    @OneOf({"none", "optional", "require"})
    @Value(hasDefault = true)
    public final String clientAuth = "none";

    /** List of ciphers to enable, separated by comma. */
    @Value(hasDefault = true)
    public String ciphers = "";

    /** SSL keystore configuration. */
    @ConfigValue
    public KeyStoreConfigurationSchema keyStore;

    /** SSL truststore configuration. */
    @ConfigValue
    public KeyStoreConfigurationSchema trustStore;
}
