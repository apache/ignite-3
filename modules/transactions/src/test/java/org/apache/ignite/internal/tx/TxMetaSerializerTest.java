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

package org.apache.ignite.internal.tx;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class TxMetaSerializerTest {
    private static final String V1_SERIALIZED_BASE64 = "Ae++QwUD6QcQ0Q8a////f///gIAE";
    private static final String V2_WITH_ZONES_SERIALIZED_BASE64 = "Au++QwEFA+kHEANmZdEPGgPKAckB////f///gIAE";

    private final TxMetaSerializer serializer = new TxMetaSerializer();

    @Test
    void serializationAndDeserializationWithoutNulls() {
        TxMeta originalMeta = new TxMeta(
                TxState.COMMITTED,
                List.of(
                        new EnlistedPartitionGroup(new ZonePartitionId(1000, 15), Set.of(100, 101)),
                        new EnlistedPartitionGroup(new ZonePartitionId(2000, 25), Set.of(200, 201))
                ),
                HybridTimestamp.MAX_VALUE
        );

        byte[] bytes = VersionedSerialization.toBytes(originalMeta, serializer);
        TxMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta, equalTo(originalMeta));
    }

    @Test
    void serializationAndDeserializationWithNulls() {
        TxMeta originalMeta = new TxMeta(
                TxState.ABANDONED,
                List.of(
                        new EnlistedPartitionGroup(new ZonePartitionId(1000, 15), Set.of(100, 101)),
                        new EnlistedPartitionGroup(new ZonePartitionId(2000, 25), Set.of(200, 201))
                ),
                null
        );

        byte[] bytes = VersionedSerialization.toBytes(originalMeta, serializer);
        TxMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta, equalTo(originalMeta));
    }

    @Test
    void v1IsNotSupported() {
        byte[] bytes = Base64.getDecoder().decode(V1_SERIALIZED_BASE64);

        assertThrows(IllegalArgumentException.class, () -> VersionedSerialization.fromBytes(bytes, serializer));
    }

    @Test
    void v2WithZonesCanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(V2_WITH_ZONES_SERIALIZED_BASE64);
        TxMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta.txState(), is(TxState.ABANDONED));
        assertThat(restoredMeta.enlistedPartitions(), contains(
                new EnlistedPartitionGroup(new ZonePartitionId(1000, 15), Set.of(100, 101)),
                new EnlistedPartitionGroup(new ZonePartitionId(2000, 25), Set.of(200, 201))
        ));
        assertThat(restoredMeta.commitTimestamp(), is(HybridTimestamp.MAX_VALUE));
    }

    @SuppressWarnings("unused")
    private String v2ZonesSerializedBase64() {
        TxMeta originalMeta = new TxMeta(
                TxState.ABANDONED,
                List.of(
                        new EnlistedPartitionGroup(new ZonePartitionId(1000, 15), Set.of(100, 101)),
                        new EnlistedPartitionGroup(new ZonePartitionId(2000, 25), Set.of(200, 201))
                ),
                HybridTimestamp.MAX_VALUE
        );
        byte[] v1Bytes = VersionedSerialization.toBytes(originalMeta, serializer);
        return Base64.getEncoder().encodeToString(v1Bytes);
    }
}
