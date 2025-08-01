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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for nodes attributes (represented with {@code Map<UUID, NodeWithAttributes>}).
 *
 * <p>The map is deserialized as a {@link ConcurrentHashMap}.
 */
public class NodesAttributesSerializer extends VersionedSerializer<Map<UUID, NodeWithAttributes>> {
    /** Serializer instance. */
    public static final NodesAttributesSerializer INSTANCE = new NodesAttributesSerializer();

    private final NodeWithAttributesSerializer nodeWithAttributesSerializer = NodeWithAttributesSerializer.INSTANCE;

    @Override
    protected void writeExternalData(Map<UUID, NodeWithAttributes> map, IgniteDataOutput out) throws IOException {
        out.writeVarInt(map.size());
        for (Map.Entry<UUID, NodeWithAttributes> entry : map.entrySet()) {
            NodeWithAttributes value = entry.getValue();

            assert entry.getKey().equals(value.nodeId()) : map;

            nodeWithAttributesSerializer.writeExternal(value, out);
        }
    }

    @Override
    protected Map<UUID, NodeWithAttributes> readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        List<NodeWithAttributes> nodes = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            nodes.add(nodeWithAttributesSerializer.readExternal(in));
        }

        return nodes.stream().collect(
                toMap(NodeWithAttributes::nodeId, identity(), (x, y) -> y, ConcurrentHashMap::new)
        );
    }

    /**
     * Serializes a map to bytes.
     *
     * @param map Map to serialize.
     */
    public static byte[] serialize(Map<UUID, NodeWithAttributes> map) {
        return VersionedSerialization.toBytes(map, INSTANCE);
    }

    /**
     * Deserializes a map from bytes.
     *
     * @param bytes Bytes.
     */
    public static Map<UUID, NodeWithAttributes> deserialize(byte[] bytes) {
        return VersionedSerialization.fromBytes(bytes, INSTANCE);
    }
}
