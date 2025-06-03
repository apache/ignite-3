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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Container of IP addresses of this node.
 */
class LocalIpAddresses {
    private static final IgniteLogger LOG = Loggers.forClass(LocalIpAddresses.class);

    private Set<InetAddress> addresses;

    void start() {
        try {
            addresses = NetworkInterface.networkInterfaces()
                    .flatMap(NetworkInterface::inetAddresses)
                    .collect(Collectors.toUnmodifiableSet());
        } catch (SocketException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Cannot get local addresses", e);
        }

        LOG.info("Local IP addresses: {}", addresses);
    }

    boolean isLocal(InetAddress addr) {
        return addresses.contains(addr);
    }
}
