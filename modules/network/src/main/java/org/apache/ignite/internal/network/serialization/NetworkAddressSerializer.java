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
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.apache.ignite.network.NetworkAddress;

/**
 * {@link VersionedSerializer} for network addresses (represented with {@code NetworkAddress} instances).
 */
public class NetworkAddressSerializer extends VersionedSerializer<NetworkAddress> {
    /** Serializer instance. */
    public static final NetworkAddressSerializer INSTANCE = new NetworkAddressSerializer();

    @Override
    protected void writeExternalData(NetworkAddress address, IgniteDataOutput out) throws IOException {
        out.writeUTF(address.host());
        out.writeInt(address.port());
    }

    @Override
    protected NetworkAddress readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        String host = in.readUTF();
        int port = in.readInt();

        return new NetworkAddress(host, port);
    }

    /**
     * Serializes address to bytes.
     *
     * @param address Address to serialize.
     */
    public static byte[] serialize(NetworkAddress address) {
        return VersionedSerialization.toBytes(address, INSTANCE);
    }

    /**
     * Deserializes an address from bytes.
     *
     * @param bytes Bytes.
     */
    public static NetworkAddress deserialize(byte[] bytes) {
        return VersionedSerialization.fromBytes(bytes, INSTANCE);
    }
}
