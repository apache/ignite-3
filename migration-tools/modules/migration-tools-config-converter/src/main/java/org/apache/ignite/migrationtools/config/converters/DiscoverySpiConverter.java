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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.registry.ConfigurationRegistryInterface;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite3.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite3.internal.network.configuration.StaticNodeFinderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DiscoverySpiConverter. */
public class DiscoverySpiConverter implements ConfigurationConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoverySpiConverter.class);

    private static Pattern PORT_RANGE_PATTERN = Pattern.compile(":(?<fp>\\d+)(..(?<lp>\\d+))?$");

    private static Collection<String> collectAddresses(String rawAddress) {
        // This method is very optimistic because the validation is performed by the configuration spi.
        String finalRawAddr = rawAddress.trim();
        Matcher m = PORT_RANGE_PATTERN.matcher(finalRawAddr);
        if (m.find()) {
            int fp = Integer.parseInt(m.group("fp"));
            String lpStr = m.group("lp");
            if (lpStr == null) {
                // Just a single port, add as is.
                return Collections.singleton(finalRawAddr);
            } else {
                // Port Range
                String hostname = finalRawAddr.substring(0, m.start());
                int lp = Integer.parseInt(lpStr);
                return IntStream.rangeClosed(fp, lp).mapToObj(p -> hostname + ":" + p).collect(Collectors.toList());
            }
        } else {
            // It's just an hostname, we add the ai2 default port.
            return Collections.singleton(finalRawAddr + ":" + 47500);
        }
    }

    @Override
    public void convert(IgniteConfiguration src,
            ConfigurationRegistryInterface registry) throws ExecutionException, InterruptedException {
        var discSpi = src.getDiscoverySpi();
        if (discSpi == null) {
            LOGGER.error("Could not find a CommunicationSpi in the source configuration.");
            return;
        }

        if (!(discSpi instanceof TcpDiscoverySpi)) {
            LOGGER.warn("CommunicationSpi is not a TcpDiscoverySpi: {}", discSpi.getClass().getName());
            return;
        }

        TcpDiscoverySpi tcpDiscoverySpi = (TcpDiscoverySpi) discSpi;
        var target = registry.getConfiguration(NetworkExtensionConfiguration.KEY).network();

        try {
            // tcpDiscoverySpi.getLocalPort() offers an unexpected behaviour. This should be caught by the tests.
            int locPort = (int) FieldUtils.readDeclaredField(tcpDiscoverySpi, "locPort", true);
            target.port().update(locPort).get();
        } catch (IllegalAccessException e) {
            LOGGER.error("Error getting TcpDiscoverySpi local port", e);
        }

        if (tcpDiscoverySpi.getLocalPortRange() > 0) {
            // TODO: Check if we can implement an additional policy for migrating the port range.
            LOGGER.error("Local Port Range in TcpDiscoverySpi will be ignored. There's no similar feature in Apache Ignite 3");
        }

        if (tcpDiscoverySpi.getIpFinder() instanceof TcpDiscoveryVmIpFinder) {
            try {
                // We cannot use the #getRegisteredAddresses because it will filter out addresses that cannot be solve during runtime.
                var rawAddresses = (Collection<String>) FieldUtils.readField(tcpDiscoverySpi.getIpFinder(), "addrs", true);
                var addrArr = rawAddresses.stream()
                        .flatMap(a -> collectAddresses(a).stream())
                        .toArray(String[]::new);

                ((StaticNodeFinderConfiguration) target.nodeFinder())
                        .netClusterNodes()
                        .update(addrArr)
                        .get();
            } catch (IllegalAccessException | ClassCastException e) {
                LOGGER.error("Error getting addresses from TcpDiscoveryVmIpFinder addrs", e);
            }
        } else {
            LOGGER.warn("Only TcpDiscoveryVmIpFinder configurations are supported, found: {}",
                    Optional.ofNullable(tcpDiscoverySpi.getIpFinder()).map(c -> c.getClass().getName()).orElse("None"));
        }
    }
}
