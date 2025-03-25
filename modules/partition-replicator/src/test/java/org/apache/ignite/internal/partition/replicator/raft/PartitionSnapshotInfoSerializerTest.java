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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class PartitionSnapshotInfoSerializerTest {
    @Test
    void normalSerialization() {
        var snapshotInfo = new PartitionSnapshotInfo(
                1,
                2,
                new LeaseInfo(123, UUID.randomUUID(), "name"),
                new byte[] {1, 2, 3},
                Set.of(1, 2, 3)
        );

        byte[] bytes = VersionedSerialization.toBytes(snapshotInfo, PartitionSnapshotInfoSerializer.INSTANCE);

        PartitionSnapshotInfo deserialized = VersionedSerialization.fromBytes(bytes, PartitionSnapshotInfoSerializer.INSTANCE);

        assertThat(deserialized, is(snapshotInfo));
    }

    @Test
    void serializationWithEmptyValues() {
        var snapshotInfo = new PartitionSnapshotInfo(
                1,
                2,
                null,
                new byte[] {1, 2, 3},
                Set.of()
        );

        byte[] bytes = VersionedSerialization.toBytes(snapshotInfo, PartitionSnapshotInfoSerializer.INSTANCE);

        PartitionSnapshotInfo deserialized = VersionedSerialization.fromBytes(bytes, PartitionSnapshotInfoSerializer.INSTANCE);

        assertThat(deserialized, is(snapshotInfo));
    }

    @Test
    void v1CanBeDeserialized() {
        var snapshotInfo = new PartitionSnapshotInfo(
                1,
                2,
                new LeaseInfo(123, new UUID(12, 34), "name"),
                new byte[] {1, 2, 3},
                Set.of(1, 2, 3)
        );

        byte[] bytes = Base64.getDecoder()
                .decode("Ae++QwEAAAAAAAAAAgAAAAAAAAABAe++Q3sAAAAAAAAADAAAAAAAAAAiAAAAAAAAAAVuYW1lBAECAwQCAwQ=");

        PartitionSnapshotInfo deserialized = VersionedSerialization.fromBytes(bytes, PartitionSnapshotInfoSerializer.INSTANCE);

        assertThat(deserialized, is(snapshotInfo));
    }
}
