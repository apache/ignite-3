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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for data nodes' maps. Deprecated, {@link DataNodesHistorySerializer} should be used instead,
 * preserved for backward compatibility.
 */
@Deprecated
public class DataNodesMapSerializer extends VersionedSerializer<Map<Node, Integer>> {
    /** Serializer instance. */
    public static final DataNodesMapSerializer INSTANCE = new DataNodesMapSerializer();

    private final NodeSerializer nodeSerializer = NodeSerializer.INSTANCE;

    @Override
    protected void writeExternalData(Map<Node, Integer> map, IgniteDataOutput out) throws IOException {
        throw new UnsupportedOperationException("Data nodes map is a legacy structure that should not be used anymore.");
    }

    @Override
    protected Map<Node, Integer> readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int size = in.readVarIntAsInt();

        Map<Node, Integer> map = new HashMap<>(IgniteUtils.capacity(size));
        for (int i = 0; i < size; i++) {
            Node node = nodeSerializer.readExternal(in);
            int count = in.readVarIntAsInt();

            map.put(node, count);
        }

        return map;
    }

    /**
     * Deserializes a map from bytes.
     *
     * @param bytes Bytes.
     */
    public static Map<Node, Integer> deserialize(byte[] bytes) {
        return VersionedSerialization.fromBytes(bytes, INSTANCE);
    }
}
