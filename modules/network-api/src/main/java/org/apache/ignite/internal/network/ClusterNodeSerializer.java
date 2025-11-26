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

import java.io.IOException;
import java.util.UUID;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;

/**
 * {@link VersionedSerializer} for {@link InternalClusterNode} instances.
 */
public class ClusterNodeSerializer extends VersionedSerializer<InternalClusterNode> {
    /** Serializer instance. */
    public static final ClusterNodeSerializer INSTANCE = new ClusterNodeSerializer();

    @Override
    protected void writeExternalData(InternalClusterNode node, IgniteDataOutput out) throws IOException {
        out.writeUuid(node.id());
        out.writeUTF(node.name());

        out.writeUTF(node.address().host());
        out.writeVarInt(node.address().port());

        NodeMetadata metadata = node.nodeMetadata();
        out.writeBoolean(metadata != null);
        if (metadata != null) {
            out.writeUTF(metadata.restHost());
            out.writeVarInt(metadata.httpPort());
            out.writeVarInt(metadata.httpsPort());
        }
    }

    @Override
    protected InternalClusterNode readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        UUID id = in.readUuid();
        String name = in.readUTF();
        NetworkAddress address = new NetworkAddress(in.readUTF(), in.readVarIntAsInt());

        boolean hasMetadata = in.readBoolean();
        NodeMetadata metadata;
        if (hasMetadata) {
            metadata = new NodeMetadata(in.readUTF(), in.readVarIntAsInt(), in.readVarIntAsInt());
        } else {
            metadata = null;
        }

        return new ClusterNodeImpl(id, name, address, metadata);
    }
}
