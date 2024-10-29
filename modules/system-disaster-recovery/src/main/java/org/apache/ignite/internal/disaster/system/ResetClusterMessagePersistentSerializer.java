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

package org.apache.ignite.internal.disaster.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * {@link VersionedSerializer} for {@link ResetClusterMessage} instances.
 */
public class ResetClusterMessagePersistentSerializer extends VersionedSerializer<ResetClusterMessage> {
    private static final SystemDisasterRecoveryMessagesFactory MESSAGES_FACTORY = new SystemDisasterRecoveryMessagesFactory();

    /** Serializer instance. */
    public static final ResetClusterMessagePersistentSerializer INSTANCE = new ResetClusterMessagePersistentSerializer();

    @Override
    protected void writeExternalData(ResetClusterMessage message, IgniteDataOutput out) throws IOException {
        writeStringSet(message.newCmgNodes(), out);
        writeStringSet(message.currentMetaStorageNodes(), out);
        out.writeUTF(message.clusterName());
        out.writeUuid(message.clusterId());
        writeNullableString(message.initialClusterConfiguration(), out);

        out.writeVarInt(message.formerClusterIds().size());
        for (UUID id : message.formerClusterIds()) {
            out.writeUuid(id);
        }

        Integer metastorageReplicationFactor = message.metastorageReplicationFactor();
        out.writeVarInt(metastorageReplicationFactor == null ? -1 : metastorageReplicationFactor);

        writeNullableString(message.conductor(), out);

        Set<String> participatingNodes = message.participatingNodes();
        if (participatingNodes == null) {
            out.writeVarInt(-1);
        } else {
            writeStringSet(participatingNodes, out);
        }
    }

    @Override
    protected ResetClusterMessage readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        Set<String> newCmgNodes = readStringSet(in);
        Set<String> currentMetaStorageNodes = readStringSet(in);
        String clusterName = in.readUTF();
        UUID clusterId = in.readUuid();
        String initialClusterConfiguration = readNullableString(in);
        List<UUID> formerClusterIds = readFormerClusterIds(in);
        Integer metastorageReplicationFactor = readNullableInteger(in);
        String conductor = readNullableString(in);
        Set<String> participatingNodes = readNullableStringSet(in);

        return MESSAGES_FACTORY.resetClusterMessage()
                .newCmgNodes(newCmgNodes)
                .currentMetaStorageNodes(currentMetaStorageNodes)
                .clusterName(clusterName)
                .clusterId(clusterId)
                .initialClusterConfiguration(initialClusterConfiguration)
                .formerClusterIds(formerClusterIds)
                .metastorageReplicationFactor(metastorageReplicationFactor)
                .conductor(conductor)
                .participatingNodes(participatingNodes)
                .build();
    }

    private static Set<String> readStringSet(int size, IgniteDataInput in) throws IOException {
        Set<String> result = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            result.add(in.readUTF());
        }

        return result;
    }

    private static List<UUID> readFormerClusterIds(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        assert length >= 0 : length;

        List<UUID> result = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            result.add(in.readUuid());
        }

        return result;
    }

    private static @Nullable Integer readNullableInteger(IgniteDataInput in) throws IOException {
        int val = in.readVarIntAsInt();
        return val == -1 ? null : val;
    }

    private static @Nullable Set<String> readNullableStringSet(IgniteDataInput in) throws IOException {
        int lengthOrMinusOne = in.readVarIntAsInt();

        if (lengthOrMinusOne == -1) {
            return null;
        }

        return readStringSet(lengthOrMinusOne, in);
    }
}
