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
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite3.client.handler.configuration.ClientConnectorExtensionConfiguration;
import org.apache.ignite3.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite3.internal.network.configuration.SslConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SslContextFactoryConverter. */
public class SslContextFactoryConverter implements ConfigurationConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SslContextFactoryConverter.class);

    @SuppressWarnings("PMD.UseEqualsToCompareStrings")
    private static void applySslConfigs(SslConfiguration target, SslContextFactory tcpCommSpi)
            throws InterruptedException, ExecutionException {
        target.keyStore().path().update(tcpCommSpi.getKeyStoreFilePath()).get();
        target.keyStore().password().update(new String(tcpCommSpi.getKeyStorePassword())).get();
        if (tcpCommSpi.getTrustStoreFilePath() != null) {
            target.trustStore().path().update(tcpCommSpi.getTrustStoreFilePath()).get();
        }
        if (tcpCommSpi.getTrustStorePassword() != null) {
            target.trustStore().password().update(new String(tcpCommSpi.getTrustStorePassword())).get();
        }

        // Checking for reference on purpose to guess if the value was defaulted or not.
        if (tcpCommSpi.getKeyAlgorithm() != SslContextFactory.DFLT_KEY_ALGORITHM) {
            target.ciphers().update(tcpCommSpi.getKeyAlgorithm());
        }

        // Check if ssl validation was disabled.
        var thrustManagers = tcpCommSpi.getTrustManagers();
        if (thrustManagers != null && thrustManagers.length == 1
                && thrustManagers[0].getClass().equals(SslContextFactory.getDisabledTrustManager().getClass())) {
            target.clientAuth().update("NONE").get();
        }
    }

    @Override
    public void convert(IgniteConfiguration src,
            ConfigurationRegistryInterface registry) throws ExecutionException, InterruptedException {
        var srcSslCf = src.getSslContextFactory();
        if (srcSslCf == null) {
            LOGGER.error("Could not find a CommunicationSpi in the source configuration.");
            return;
        }

        if (!(srcSslCf instanceof SslContextFactory)) {
            LOGGER.warn("SslContextFactory is not a SslContextFactory: {}", srcSslCf.getClass().getName());
            return;
        }

        SslContextFactory sslCtxFactory = (SslContextFactory) srcSslCf;

        var discoTarget = registry.getConfiguration(NetworkExtensionConfiguration.KEY).network().ssl();
        applySslConfigs(discoTarget, sslCtxFactory);

        var jdbcTarget = registry.getConfiguration(ClientConnectorExtensionConfiguration.KEY).clientConnector().ssl();
        applySslConfigs(jdbcTarget, sslCtxFactory);
    }
}
