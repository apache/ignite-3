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

package org.apache.ignite.internal.distributionzones;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for logical topology sets (represented with {@code Set<NodeWithAttributes} instances).
 */
public class LogicalTopologySetSerializer extends VersionedSerializer<Set<NodeWithAttributes>> {
    /** Serializer instance. */
    public static final LogicalTopologySetSerializer INSTANCE = new LogicalTopologySetSerializer();

    private final NodeWithAttributesSerializer nodeWithAttributesSerializer = NodeWithAttributesSerializer.INSTANCE;

    @Override
    protected void writeExternalData(Set<NodeWithAttributes> topology, IgniteDataOutput out) throws IOException {
        out.writeVarInt(topology.size());
        for (NodeWithAttributes node : topology) {
            nodeWithAttributesSerializer.writeExternal(node, out);
        }
    }

    @Override
    protected Set<NodeWithAttributes> readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Set<NodeWithAttributes> topology = new HashSet<>(IgniteUtils.capacity(length));
        for (int i = 0; i < length; i++) {
            topology.add(nodeWithAttributesSerializer.readExternal(in));
        }

        return topology;
    }

    /**
     * Serializes a set to bytes.
     *
     * @param set Set to serialize.
     */
    public static byte[] serialize(Set<NodeWithAttributes> set) {
        return VersionedSerialization.toBytes(set, INSTANCE);
    }

    /**
     * Deserializes a set from bytes.
     *
     * @param bytes Bytes.
     */
    public static Set<NodeWithAttributes> deserialize(byte[] bytes) {
        return VersionedSerialization.fromBytes(bytes, INSTANCE);
    }
}
