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

import java.util.Base64;
import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class TxMetaSerializerTest {
    private final TxMetaSerializer serializer = new TxMetaSerializer();

    @Test
    void serializationAndDeserialization() {
        TxMeta originalMeta = new TxMeta(
                TxState.ABANDONED,
                List.of(new TablePartitionId(1000, 15), new TablePartitionId(2000, 25)),
                HybridTimestamp.MAX_VALUE
        );

        byte[] bytes = VersionedSerialization.toBytes(originalMeta, serializer);
        TxMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta, equalTo(originalMeta));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwUD6QcQ0Q8a/////////38=");
        TxMeta restoredMeta = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMeta.txState(), is(TxState.ABANDONED));
        assertThat(restoredMeta.enlistedPartitions(), contains(new TablePartitionId(1000, 15), new TablePartitionId(2000, 25)));
        assertThat(restoredMeta.commitTimestamp(), is(HybridTimestamp.MAX_VALUE));
    }
}
