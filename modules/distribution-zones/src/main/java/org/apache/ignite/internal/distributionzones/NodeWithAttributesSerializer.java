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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link NodeWithAttributes} instances.
 */
public class NodeWithAttributesSerializer extends VersionedSerializer<NodeWithAttributes> {
    /** Serializer instance. */
    public static final NodeWithAttributesSerializer INSTANCE = new NodeWithAttributesSerializer();

    private final NodeSerializer nodeSerializer = NodeSerializer.INSTANCE;

    @Override
    protected void writeExternalData(NodeWithAttributes node, IgniteDataOutput out) throws IOException {
        nodeSerializer.writeExternal(node.node(), out);

        out.writeVarInt(node.userAttributes().size());
        for (Map.Entry<String, String> attrEntry : node.userAttributes().entrySet()) {
            out.writeUTF(attrEntry.getKey());
            out.writeUTF(attrEntry.getValue());
        }

        out.writeVarInt(node.storageProfiles().size());
        for (String profile : node.storageProfiles()) {
            out.writeUTF(profile);
        }
    }

    @Override
    protected NodeWithAttributes readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        Node node = nodeSerializer.readExternal(in);
        Map<String, String> userAttributes = readUserAttributes(in);
        List<String> storageProfiles = readStorageProfiles(in);

        return new NodeWithAttributes(node.nodeName(), node.nodeId(), userAttributes, storageProfiles);
    }

    private static Map<String, String> readUserAttributes(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Map<String, String> attrs = new HashMap<>(IgniteUtils.capacity(length));
        for (int i = 0; i < length; i++) {
            attrs.put(in.readUTF(), in.readUTF());
        }

        return attrs;
    }

    private static List<String> readStorageProfiles(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        List<String> storageProfiles = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            storageProfiles.add(in.readUTF());
        }

        return storageProfiles;
    }
}
