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
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite3.internal.network.configuration.NetworkExtensionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CommunicationSpiConverter. */
public class CommunicationSpiConverter implements ConfigurationConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommunicationSpiConverter.class);

    @Override public void convert(IgniteConfiguration src,
            ConfigurationRegistryInterface registry) throws ExecutionException, InterruptedException {
        var commSpi = src.getCommunicationSpi();
        if (commSpi == null) {
            LOGGER.error("Could not find a CommunicationSpi in the source configuration.");
            return;
        }

        if (!(commSpi instanceof TcpCommunicationSpi)) {
            LOGGER.warn("CommunicationSpi is not a TcpCommunicationSpi: {}", commSpi.getClass().getName());
            return;
        }

        TcpCommunicationSpi tcpCommSpi = (TcpCommunicationSpi) commSpi;
        var target = registry.getConfiguration(NetworkExtensionConfiguration.KEY).network();

        target.port().update(tcpCommSpi.getLocalPort()).get();
        if (tcpCommSpi.getLocalPortRange() > 0) {
            // TODO: Check if we can implement an additional policy for migrating the port range.
            LOGGER.error("Local Port Range in TcpCommunicationSpi will be ignored. There's no similar feature in Apache Ignite 3");
        }

        // TODO: Check if this is the correct delay config
        target.inbound().tcpNoDelay().update(tcpCommSpi.isTcpNoDelay()).get();
    }
}
