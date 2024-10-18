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
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link Augmentation} instances.
 */
public class AugmentationSerializer extends VersionedSerializer<Augmentation> {
    /** Serializer instance. */
    public static final AugmentationSerializer INSTANCE = new AugmentationSerializer();

    private final NodeSerializer nodeSerializer = NodeSerializer.INSTANCE;

    @Override
    protected void writeExternalData(Augmentation augmentation, IgniteDataOutput out) throws IOException {
        out.writeVarInt(augmentation.nodes().size());
        for (Node node : augmentation.nodes()) {
            nodeSerializer.writeExternal(node, out);
        }

        out.writeBoolean(augmentation.addition());
    }

    @Override
    protected Augmentation readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        Set<Node> nodes = readNodes(in);
        boolean addition = in.readBoolean();

        return new Augmentation(nodes, addition);
    }

    private Set<Node> readNodes(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Set<Node> nodes = new HashSet<>(IgniteUtils.capacity(length));
        for (int i = 0; i < length; i++) {
            nodes.add(nodeSerializer.readExternal(in));
        }

        return nodes;
    }
}
