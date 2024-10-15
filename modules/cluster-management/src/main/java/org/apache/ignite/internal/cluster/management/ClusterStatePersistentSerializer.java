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

package org.apache.ignite.internal.cluster.management;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * {@link VersionedSerializer} for {@link ClusterState} instance.
 */
public class ClusterStatePersistentSerializer extends VersionedSerializer<ClusterState> {
    private static final CmgMessagesFactory CMG_MSGS_FACTORY = new CmgMessagesFactory();

    /** Serializer instance. */
    public static final ClusterStatePersistentSerializer INSTANCE = new ClusterStatePersistentSerializer();

    @Override
    protected void writeExternalData(ClusterState state, IgniteDataOutput out) throws IOException {
        writeStringSet(state.cmgNodes(), out);
        writeStringSet(state.metaStorageNodes(), out);
        out.writeUTF(state.version());
        out.writeUTF(state.clusterTag().clusterName());
        out.writeUuid(state.clusterTag().clusterId());
        writeNullableString(state.initialClusterConfiguration(), out);

        out.writeLength(state.formerClusterIds() == null ? 0 : state.formerClusterIds().size());
        if (state.formerClusterIds() != null) {
            for (UUID clusterId : state.formerClusterIds()) {
                out.writeUuid(clusterId);
            }
        }
    }

    private static void writeStringSet(Set<String> strings, IgniteDataOutput out) throws IOException {
        out.writeLength(strings.size());
        for (String str : strings) {
            out.writeUTF(str);
        }
    }

    private static void writeNullableString(@Nullable String str, IgniteDataOutput out) throws IOException {
        int lengthOrMinusOne = str == null ? -1 : str.length();
        int lengthPlusOneOrZero = lengthOrMinusOne + 1;

        out.writeLength(lengthPlusOneOrZero);
        if (str != null) {
            out.writeByteArray(str.getBytes(UTF_8));
        }
    }

    @Override
    protected ClusterState readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        return CMG_MSGS_FACTORY.clusterState()
                .cmgNodes(readStringSet(in))
                .metaStorageNodes(readStringSet(in))
                .version(in.readUTF())
                .clusterTag(ClusterTag.clusterTag(CMG_MSGS_FACTORY, in.readUTF(), in.readUuid()))
                .initialClusterConfiguration(readNullableString(in))
                .formerClusterIds(readFormerClusterIds(in))
                .build();
    }

    private static Set<String> readStringSet(IgniteDataInput in) throws IOException {
        int size = in.readLength();

        Set<String> result = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            result.add(in.readUTF());
        }

        return result;
    }

    private static @Nullable String readNullableString(IgniteDataInput in) throws IOException {
        int lengthPlusOne = in.readLength();
        if (lengthPlusOne == 0) {
            return null;
        }

        return new String(in.readByteArray(lengthPlusOne - 1), UTF_8);
    }

    private static @Nullable List<UUID> readFormerClusterIds(IgniteDataInput in) throws IOException {
        int length = in.readLength();

        if (length == 0) {
            return null;
        }

        List<UUID> result = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            result.add(in.readUuid());
        }

        return result;
    }
}
