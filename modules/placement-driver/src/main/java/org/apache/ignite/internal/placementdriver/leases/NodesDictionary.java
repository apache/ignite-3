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

package org.apache.ignite.internal.placementdriver.leases;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Represents a dictionary of nodes mentioned in a {@link LeaseBatch}.
 *
 * <p>Has two parts: table of node names and table of nodes (these are node ID + node name index pairs).
 */
final class NodesDictionary {
    private final List<String> nameIndexToName = new ArrayList<>();
    private final Object2IntMap<String> nameToNameIndex = new Object2IntOpenHashMap<>();

    private final List<UUID> nodeIndexToId = new ArrayList<>();
    private final Object2IntMap<UUID> idToNodeIndex = new Object2IntOpenHashMap<>();
    private final Object2IntMap<UUID> idToNameIndex = new Object2IntOpenHashMap<>();

    NodesDictionary() {
        nameToNameIndex.defaultReturnValue(-1);
        idToNodeIndex.defaultReturnValue(-1);
        idToNameIndex.defaultReturnValue(-1);
    }

    int putNode(UUID id, String name) {
        if (idToNodeIndex.containsKey(id)) {
            return idToNodeIndex.getInt(id);
        } else {
            int nameIndex = putName(name);

            int nodeIndex = idToNodeIndex.size();
            nodeIndexToId.add(id);
            idToNodeIndex.put(id, nodeIndex);
            idToNameIndex.put(id, nameIndex);

            return nodeIndex;
        }
    }

    int putName(String name) {
        int nameIndex;
        if (nameToNameIndex.containsKey(name)) {
            nameIndex = nameToNameIndex.getInt(name);
        } else {
            nameIndex = nameIndexToName.size();
            nameIndexToName.add(name);
            nameToNameIndex.put(name, nameIndex);
        }

        return nameIndex;
    }

    String getName(int nameIndex) {
        return nameIndexToName.get(nameIndex);
    }

    UUID getNodeId(int nodeIndex) {
        return nodeIndexToId.get(nodeIndex);
    }

    String getNodeName(int nodeIndex) {
        UUID id = getNodeId(nodeIndex);
        int nameIndex = idToNameIndex.getInt(id);
        return getName(nameIndex);
    }

    int getNameIndex(String name) {
        return nameToNameIndex.getInt(name);
    }

    int getNodeIndex(UUID leaseHolderId) {
        return idToNodeIndex.getInt(leaseHolderId);
    }

    void writeTo(IgniteDataOutput out) throws IOException {
        out.writeVarInt(nameIndexToName.size());
        for (String name : nameIndexToName) {
            out.writeUTF(name);
        }

        out.writeVarInt(nodeIndexToId.size());
        for (UUID id : nodeIndexToId) {
            out.writeUuid(id);
            out.writeVarInt(idToNameIndex.getInt(id));
        }
    }

    static NodesDictionary readFrom(IgniteDataInput in) throws IOException {
        NodesDictionary dict = new NodesDictionary();

        int namesCount = in.readVarIntAsInt();
        for (int i = 0; i < namesCount; i++) {
            dict.putName(in.readUTF());
        }

        int nodesCount = in.readVarIntAsInt();
        for (int i = 0; i < nodesCount; i++) {
            UUID id = in.readUuid();
            int nameIndex = in.readVarIntAsInt();

            String name = dict.nameIndexToName.get(nameIndex);
            dict.putNode(id, name);
        }

        return dict;
    }

    int nameCount() {
        return nameIndexToName.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodesDictionary that = (NodesDictionary) o;
        return nameIndexToName.equals(that.nameIndexToName)
                && nameToNameIndex.equals(that.nameToNameIndex)
                && nodeIndexToId.equals(that.nodeIndexToId)
                && idToNodeIndex.equals(that.idToNodeIndex)
                && idToNameIndex.equals(that.idToNameIndex);
    }

    @Override
    public int hashCode() {
        int result = nameIndexToName.hashCode();
        result = 31 * result + nameToNameIndex.hashCode();
        result = 31 * result + nodeIndexToId.hashCode();
        result = 31 * result + idToNodeIndex.hashCode();
        result = 31 * result + idToNameIndex.hashCode();
        return result;
    }
}
