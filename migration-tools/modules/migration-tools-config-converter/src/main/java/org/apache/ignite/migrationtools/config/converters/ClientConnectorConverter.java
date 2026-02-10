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

package org.apache.ignite.migrationtools.config.converters;

import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.registry.ConfigurationRegistryInterface;
import org.apache.ignite3.client.handler.configuration.ClientConnectorExtensionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ClientConnectorConverter. */
public class ClientConnectorConverter implements ConfigurationConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnectorConverter.class);

    @Override
    public void convert(IgniteConfiguration src,
            ConfigurationRegistryInterface registry) throws ExecutionException, InterruptedException {
        var clientConnCfg = src.getClientConnectorConfiguration();
        if (clientConnCfg == null) {
            LOGGER.warn("Could not find a ClientConnectorConfiguration in the source configuration.");
            return;
        }

        var target = registry.getConfiguration(ClientConnectorExtensionConfiguration.KEY).clientConnector();

        target.port().update(clientConnCfg.getPort()).get();

        if (clientConnCfg.getHandshakeTimeout() != org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_HANDSHAKE_TIMEOUT) {
            target.connectTimeoutMillis().update((int) Math.min(Integer.MAX_VALUE, clientConnCfg.getHandshakeTimeout())).get();
        }

        if (clientConnCfg.getIdleTimeout() > 0) {
            target.idleTimeoutMillis().update(clientConnCfg.getIdleTimeout()).get();
        }
    }
}
