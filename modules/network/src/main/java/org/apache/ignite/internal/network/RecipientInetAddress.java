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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Internet address of the recipient for sending network messages. Will contain {@link null} address if the recipient is the current ignite
 * node.
 */
class RecipientInetAddress {
    private static final RecipientInetAddress SELF = new RecipientInetAddress(null);

    private final @Nullable InetSocketAddress address;

    private RecipientInetAddress(@Nullable InetSocketAddress address) {
        this.address = address;
    }

    /** Returns the internet address of the recipient, {@link null} if the recipient is the current ignite node. */
    @Nullable InetSocketAddress address() {
        return address;
    }

    /**
     * Creates new instance of {@link RecipientInetAddress}.
     *
     * @param localBindAddress Internet address to which current ignite node bound (it may be the 'any local' address aka wildcard).
     * @param recipientAddress Recipient network address.
     * @param localIpAddresses Container of local IP addresses of this node.
     */
    static RecipientInetAddress create(
            InetSocketAddress localBindAddress,
            NetworkAddress recipientAddress,
            LocalIpAddresses localIpAddresses
    ) {
        if (localBindAddress.getPort() != recipientAddress.port()) {
            return new RecipientInetAddress(createResolved(recipientAddress));
        }

        // For optimization, we will check the addresses without resolving the address of the target node.
        if (Objects.equals(localBindAddress.getHostName(), recipientAddress.host())) {
            return SELF;
        }

        InetSocketAddress resolvedRecipientAddress = createResolved(recipientAddress);
        InetAddress recipientInetAddress = resolvedRecipientAddress.getAddress();

        if (Objects.equals(localBindAddress.getAddress(), recipientInetAddress)) {
            return SELF;
        }

        if (recipientInetAddress.isAnyLocalAddress() || recipientInetAddress.isLoopbackAddress()) {
            return SELF;
        }

        if (localIpAddresses.isLocal(recipientInetAddress)) {
            return SELF;
        }

        return new RecipientInetAddress(resolvedRecipientAddress);
    }

    private static InetSocketAddress createResolved(NetworkAddress address) {
        return new InetSocketAddress(address.host(), address.port());
    }
}
