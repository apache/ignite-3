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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for topology augmentation maps represented with {@code ConcurrentSkipListMap<Long, Augmentation>}.
 */
public class TopologyAugmentationMapSerializer extends VersionedSerializer<ConcurrentSkipListMap<Long, Augmentation>> {
    /** Serializer instance. */
    public static final TopologyAugmentationMapSerializer INSTANCE = new TopologyAugmentationMapSerializer();

    private final AugmentationSerializer augmentationSerializer = AugmentationSerializer.INSTANCE;

    @Override
    protected void writeExternalData(ConcurrentSkipListMap<Long, Augmentation> map, IgniteDataOutput out) throws IOException {
        out.writeVarInt(map.size());
        for (Entry<Long, Augmentation> entry : map.entrySet()) {
            out.writeVarInt(entry.getKey());
            augmentationSerializer.writeExternal(entry.getValue(), out);
        }
    }

    @Override
    protected ConcurrentSkipListMap<Long, Augmentation> readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        ConcurrentSkipListMap<Long, Augmentation> map = new ConcurrentSkipListMap<>();
        for (int i = 0; i < length; i++) {
            map.put(in.readVarInt(), augmentationSerializer.readExternal(in));
        }

        return map;
    }

    /**
     * Serializes a map to bytes.
     *
     * @param map Map to serialize.
     */
    public static byte[] serialize(ConcurrentSkipListMap<Long, Augmentation> map) {
        return VersionedSerialization.toBytes(map, INSTANCE);
    }

    /**
     * Deserializes a map from bytes.
     *
     * @param bytes Bytes.
     */
    public static ConcurrentSkipListMap<Long, Augmentation> deserialize(byte[] bytes) {
        return VersionedSerialization.fromBytes(bytes, INSTANCE);
    }
}
