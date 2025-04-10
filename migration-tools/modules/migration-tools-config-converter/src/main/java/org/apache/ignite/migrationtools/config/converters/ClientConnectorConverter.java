/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
