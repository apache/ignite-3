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

package org.apache.ignite.internal.cluster.management.topology.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.network.ClusterNodeSerializer;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link LogicalNode} instances.
 */
public class LogicalNodeSerializer extends VersionedSerializer<LogicalNode> {
    /** Serializer instance. */
    public static final LogicalNodeSerializer INSTANCE = new LogicalNodeSerializer();

    private final ClusterNodeSerializer clusterNodeSerializer = ClusterNodeSerializer.INSTANCE;

    @Override
    protected void writeExternalData(LogicalNode node, IgniteDataOutput out) throws IOException {
        clusterNodeSerializer.writeExternal(node, out);

        writeStringToStringMap(node.userAttributes(), out);
        writeStringToStringMap(node.systemAttributes(), out);

        out.writeVarInt(node.storageProfiles().size());
        for (String profile : node.storageProfiles()) {
            out.writeUTF(profile);
        }
    }

    private static void writeStringToStringMap(Map<String, String> map, IgniteDataOutput output) throws IOException {
        output.writeVarInt(map.size());

        for (Entry<String, String> entry : map.entrySet()) {
            output.writeUTF(entry.getKey());
            output.writeUTF(entry.getValue());
        }
    }

    @Override
    protected LogicalNode readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        InternalClusterNode node = clusterNodeSerializer.readExternal(in);

        Map<String, String> userAttributes = readStringToStringMap(in);
        Map<String, String> systemAttributes = readStringToStringMap(in);
        List<String> storageProfiles = readStringList(in);

        return new LogicalNode(node, userAttributes, systemAttributes, storageProfiles);
    }

    private static Map<String, String> readStringToStringMap(IgniteDataInput in) throws IOException {
        int size = in.readVarIntAsInt();

        var map = new HashMap<String, String>(IgniteUtils.capacity(size));
        for (int i = 0; i < size; i++) {
            map.put(in.readUTF(), in.readUTF());
        }

        return map;
    }

    private static List<String> readStringList(IgniteDataInput in) throws IOException {
        int size = in.readVarIntAsInt();

        var list = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readUTF());
        }

        return list;
    }
}
