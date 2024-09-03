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

import static java.util.stream.Collectors.toList;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
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
    public List<NetworkAddress> findNodes() {
        return addresses.parallelStream()
                .flatMap(
                        originalAddress -> Arrays.stream(resolveAll(originalAddress.host()))
                                .map(ip -> new NetworkAddress(ip, originalAddress.port()))
                )
                .collect(toList());
    }

    private static String[] resolveAll(String host) {
        InetAddress[] inetAddresses;
        try {
            inetAddresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            LOG.warn("Cannot resolve {}", host);
            return ArrayUtils.STRING_EMPTY_ARRAY;
        }

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
