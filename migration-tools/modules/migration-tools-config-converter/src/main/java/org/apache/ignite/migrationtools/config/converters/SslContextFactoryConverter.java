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
