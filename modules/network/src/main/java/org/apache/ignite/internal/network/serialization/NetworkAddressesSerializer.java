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

package org.apache.ignite.internal.network.serialization;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.apache.ignite.network.NetworkAddress;

/**
 * {@link VersionedSerializer} for network addresses (represented with {@code Set<NetworkAddress>} instances).
 */
public class NetworkAddressesSerializer extends VersionedSerializer<Set<NetworkAddress>> {
    /** Serializer instance. */
    public static final NetworkAddressesSerializer INSTANCE = new NetworkAddressesSerializer();

    private final NetworkAddressSerializer networkAddressSerializer = NetworkAddressSerializer.INSTANCE;

    @Override
    protected void writeExternalData(Set<NetworkAddress> addresses, IgniteDataOutput out) throws IOException {
        out.writeVarInt(addresses.size());
        for (NetworkAddress address : addresses) {
            networkAddressSerializer.writeExternal(address, out);
        }
    }

    @Override
    protected Set<NetworkAddress> readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Set<NetworkAddress> addresses = new HashSet<>(IgniteUtils.capacity(length));
        for (int i = 0; i < length; i++) {
            addresses.add(networkAddressSerializer.readExternal(in));
        }

        return addresses;
    }

    /**
     * Serializes addresses to bytes.
     *
     * @param addresses Addresses to serialize.
     */
    public static byte[] serialize(Set<NetworkAddress> addresses) {
        return VersionedSerialization.toBytes(addresses, INSTANCE);
    }

    /**
     * Deserializes addresses from bytes.
     *
     * @param bytes Bytes.
     */
    public static Set<NetworkAddress> deserialize(byte[] bytes) {
        return VersionedSerialization.fromBytes(bytes, INSTANCE);
    }
}
