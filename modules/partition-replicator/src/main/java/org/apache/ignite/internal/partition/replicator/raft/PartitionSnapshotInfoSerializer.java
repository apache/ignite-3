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

package org.apache.ignite.internal.partition.replicator.raft;

import java.io.IOException;
import java.util.Set;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.lease.LeaseInfoSerializer;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Serializer implementation for {@link PartitionSnapshotInfo}.
 */
public class PartitionSnapshotInfoSerializer extends VersionedSerializer<PartitionSnapshotInfo> {
    /** Serializer instance. */
    public static final PartitionSnapshotInfoSerializer INSTANCE = new PartitionSnapshotInfoSerializer();

    @Override
    protected void writeExternalData(PartitionSnapshotInfo snapshotInfo, IgniteDataOutput out) throws IOException {
        out.writeLong(snapshotInfo.lastAppliedIndex());
        out.writeLong(snapshotInfo.lastAppliedTerm());

        writeNullableObject(snapshotInfo.leaseInfo(), LeaseInfoSerializer.INSTANCE, out);

        writeByteArray(snapshotInfo.configurationBytes(), out);

        writeVarIntSet(snapshotInfo.tableIds(), out);
    }

    @Override
    protected PartitionSnapshotInfo readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        long lastAppliedIndex = in.readLong();
        long lastAppliedTerm = in.readLong();

        LeaseInfo leaseInfo = readNullableObject(LeaseInfoSerializer.INSTANCE, in);

        byte[] configuration = readByteArray(in);

        Set<Integer> tableIds = readVarIntSet(in);

        return new PartitionSnapshotInfo(lastAppliedIndex, lastAppliedTerm, leaseInfo, configuration, tableIds);
    }

    private static void writeByteArray(byte[] array, IgniteDataOutput out) throws IOException {
        out.writeVarInt(array.length);
        out.writeByteArray(array);
    }

    private static <T> void writeNullableObject(
            @Nullable T object,
            VersionedSerializer<T> serializer,
            IgniteDataOutput out
    ) throws IOException {
        if (object == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            serializer.writeExternal(object, out);
        }
    }

    private static byte[] readByteArray(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        return in.readByteArray(length);
    }

    private static <T> @Nullable T readNullableObject(VersionedSerializer<T> serializer, IgniteDataInput in) throws IOException {
        boolean isNotNull = in.readBoolean();

        return isNotNull ? serializer.readExternal(in) : null;
    }
}
