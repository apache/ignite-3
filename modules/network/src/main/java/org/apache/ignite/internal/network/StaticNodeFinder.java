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

package org.apache.ignite.internal.network;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.lang.ErrorGroups.Network.ADDRESS_UNRESOLVED_ERR;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.network.NetworkAddress;

/**
 * {@code NodeFinder} implementation that encapsulates a predefined list of network addresses and/or host names.
 *
 * <p>IP addresses are returned as is (up to a format change).
 *
 * <p>Names are resolved. If a name is resolved to one or more addresses, all of them will be returned. If a name is resolved
 * to nothing, this name will not add anything to the output; no exception will be thrown in such case.
 *
 * <p>If a loopback address is among the resolved addresses, only this address is contributed for this name.
 */
public class StaticNodeFinder implements NodeFinder {
    private static final IgniteLogger LOG = Loggers.forClass(StaticNodeFinder.class);
    private static final long RETRY_WAIT_FACTOR = 500;

    /** List of seed cluster members. */
    private final List<NetworkAddress> addresses;

    /**
     * Constructor.
     *
     * @param addresses Addresses of initial cluster members.
     */
    public StaticNodeFinder(List<NetworkAddress> addresses) {
        this.addresses = addresses;
    }

    @Override
    public Collection<NetworkAddress> findNodes() {
        if (addresses.isEmpty()) {
            return Set.of();
        }

        Collection<NetworkAddress> networkAddresses = addresses.parallelStream()
                .flatMap(
                        originalAddress -> Arrays.stream(resolveAll(originalAddress.host()))
                                .map(ip -> new NetworkAddress(ip, originalAddress.port()))
                )
                .collect(toSet());

        if (networkAddresses.isEmpty()) {
            throw new IgniteInternalException(ADDRESS_UNRESOLVED_ERR, "No network addresses resolved through any provided names");
        }

        return networkAddresses;
    }

    @Override
    public void start() {
        // No-op
    }

    private static String[] resolveAll(String host) {
        InetAddress[] inetAddresses = null;

        final int maxTries = 3;
        int tryCount = 0;
        boolean resolved = false;

        do {
            tryCount++;

            try {
                inetAddresses = InetAddress.getAllByName(host);
                resolved = true;
            } catch (UnknownHostException e) {
                if (tryCount == maxTries) {
                    LOG.warn("Cannot resolve {}", host);
                    return ArrayUtils.STRING_EMPTY_ARRAY;
                }

                try {
                    Thread.sleep(tryCount * RETRY_WAIT_FACTOR);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();

                    return ArrayUtils.STRING_EMPTY_ARRAY;
                }
            }
        } while (!resolved);

        assert inetAddresses != null;
        String[] addresses = new String[inetAddresses.length];
        for (int i = 0; i < inetAddresses.length; i++) {
            InetAddress inetAddress = inetAddresses[i];

            if (inetAddress.isLoopbackAddress()) {
                // If it's a loopback address (like 127.0.0.1), only return this address.
                return new String[]{inetAddress.getHostAddress()};
            }

            addresses[i] = inetAddress.getHostAddress();
        }

        return addresses;
    }
}
