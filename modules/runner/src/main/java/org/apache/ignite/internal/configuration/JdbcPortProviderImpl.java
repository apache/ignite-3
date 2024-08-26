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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.client.handler.configuration.ClientConnectorExtensionConfiguration;
import org.apache.ignite.internal.rest.node.JdbcPortProvider;

/**
 * Provider of client port from node configuration.
 */
public class JdbcPortProviderImpl implements JdbcPortProvider {

    private final ConfigurationRegistry configurationRegistry;

    public JdbcPortProviderImpl(ConfigurationRegistry configurationRegistry) {
        this.configurationRegistry = configurationRegistry;
    }

    @Override
    public int jdbcPort() {
        return configurationRegistry.getConfiguration(ClientConnectorExtensionConfiguration.KEY).clientConnector().port().value();
    }
}
