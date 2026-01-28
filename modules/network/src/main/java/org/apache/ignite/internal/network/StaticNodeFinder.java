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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

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

    private static final long RETRY_WAIT_BASE_MILLIS = 500;

    /** List of seed cluster members. */
    private final Set<NetworkAddress> addresses;

    private final int nameResolutionAttempts;

    private final HostNameResolver hostNameResolver;

    /**
     * Class for resolving host names.
     *
     * <p>Needed for writing cleaner tests.
     */
    @FunctionalInterface
    public interface HostNameResolver {
        /**
         * Given the name of a host, returns an array of its IP addresses, based on the configured name service on the system.
         */
        InetAddress[] getAllByName(String host) throws UnknownHostException;
    }

    /**
     * Constructor.
     *
     * @param addresses Addresses of initial cluster members.
     */
    @TestOnly
    public StaticNodeFinder(List<NetworkAddress> addresses) {
        this(addresses, 1);
    }

    /**
     * Constructor.
     *
     * @param addresses Addresses of initial cluster members.
     * @param nameResolutionAttempts Number of attempts to resolve the host names from the {@code addresses} list.
     */
    public StaticNodeFinder(Collection<NetworkAddress> addresses, int nameResolutionAttempts) {
        this(addresses, InetAddress::getAllByName, nameResolutionAttempts);
    }

    /**
     * Constructor.
     *
     * @param addresses Addresses of initial cluster members.
     * @param hostNameResolver Host name resolver.
     * @param nameResolutionAttempts Number of attempts to resolve the host names from the {@code addresses} list.
     */
    @VisibleForTesting
    public StaticNodeFinder(Collection<NetworkAddress> addresses, HostNameResolver hostNameResolver, int nameResolutionAttempts) {
        this.addresses = Set.copyOf(addresses);
        this.hostNameResolver = hostNameResolver;
        this.nameResolutionAttempts = nameResolutionAttempts;
    }

    @Override
    public Collection<NetworkAddress> findNodes() {
        if (addresses.isEmpty()) {
            return Set.of();
        }

        Collection<NetworkAddress> networkAddresses = addresses.parallelStream()
                .flatMap(originalAddress -> resolveAddress(originalAddress).stream())
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

    private List<NetworkAddress> resolveAddress(NetworkAddress address) {
        InetAddress[] inetAddresses = resolveInetAddresses(address.host());

        if (inetAddresses.length == 0) {
            return List.of();
        }

        var addresses = new ArrayList<NetworkAddress>(inetAddresses.length);

        for (InetAddress inetAddress : inetAddresses) {
            if (inetAddress.isLoopbackAddress()) {
                // If it's a loopback address (like 127.0.0.1), only return this address.
                return List.of(new NetworkAddress(inetAddress.getHostAddress(), address.port()));
            }

            addresses.add(new NetworkAddress(inetAddress.getHostAddress(), address.port()));
        }

        return addresses;
    }

    private InetAddress[] resolveInetAddresses(String host) {
        try {
            int tryCount = 1;

            while (true) {
                try {
                    return hostNameResolver.getAllByName(host);
                } catch (UnknownHostException e) {
                    LOG.warn("Cannot resolve host \"{}\" (attempt {}/{}).", host, tryCount, nameResolutionAttempts);

                    if (tryCount >= nameResolutionAttempts) {
                        break;
                    }

                    //noinspection BusyWait
                    Thread.sleep(tryCount * RETRY_WAIT_BASE_MILLIS);

                    tryCount++;
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();

            LOG.warn("Cannot resolve host \"{}\" (interrupted).", host);
        }

        return new InetAddress[0];
    }
}
