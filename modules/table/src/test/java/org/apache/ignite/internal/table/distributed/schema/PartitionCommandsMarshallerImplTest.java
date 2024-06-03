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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.NO_POOL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.datareplication.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.datareplication.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.datareplication.network.command.FinishTxCommand;
import org.apache.ignite.internal.datareplication.network.command.FinishTxCommandSerializationFactory;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommandSerializationFactory;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.junit.jupiter.api.Test;

class PartitionCommandsMarshallerImplTest {
    private final PartitionReplicationMessagesFactory tableMessagesFactory = new PartitionReplicationMessagesFactory();
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    private final MessageSerializationRegistry registry = new MessageSerializationRegistryImpl();

    {
        // For a command that has required catalog version property.
        registry.registerFactory(
                PartitionReplicationMessageGroup.GROUP_TYPE,
                PartitionReplicationMessageGroup.Commands.FINISH_TX,
                new FinishTxCommandSerializationFactory(tableMessagesFactory)
        );

        // For a command that does not have required catalog version property.
        registry.registerFactory(
                ReplicaMessageGroup.GROUP_TYPE,
                ReplicaMessageGroup.SAFE_TIME_SYNC_COMMAND,
                new SafeTimeSyncCommandSerializationFactory(replicaMessagesFactory)
        );
    }

    private final OptimizedMarshaller standardMarshaller = new OptimizedMarshaller(registry, NO_POOL);

    private final PartitionCommandsMarshallerImpl partitionCommandsMarshaller = new PartitionCommandsMarshallerImpl(registry, NO_POOL);

    @Test
    void marshalPrependsWithZeroForNotCatalogLevelAware() {
        NetworkMessage obj = commandWithoutRequiredCatalogVersion();

        byte[] standardResult = standardMarshaller.marshall(obj);
        byte[] enrichedResult = partitionCommandsMarshaller.marshall(obj);

        assertThat(enrichedResult.length, is(standardResult.length + 1));
        assertThat(enrichedResult[0], is((byte) 0));
        assertThat(Arrays.copyOfRange(enrichedResult, 1, enrichedResult.length), is(equalTo(standardResult)));
    }

    private SafeTimeSyncCommand commandWithoutRequiredCatalogVersion() {
        return replicaMessagesFactory.safeTimeSyncCommand().build();
    }

    @Test
    void marshalPrependsWithCatalogVersionForCatalogLevelAware() {
        Object obj = commandWithRequiredCatalogVersion(42);

        byte[] standardResult = standardMarshaller.marshall(obj);
        byte[] enrichedResult = partitionCommandsMarshaller.marshall(obj);

        assertThat(enrichedResult.length, is(standardResult.length + 1));
        assertThat(enrichedResult[0], is((byte) 43));
        assertThat(Arrays.copyOfRange(enrichedResult, 1, enrichedResult.length), is(equalTo(standardResult)));
    }

    private FinishTxCommand commandWithRequiredCatalogVersion(int requiredCatalogVersion) {
        return tableMessagesFactory.finishTxCommand()
                .txId(UUID.randomUUID())
                .partitionIds(List.of())
                .requiredCatalogVersion(requiredCatalogVersion)
                .build();
    }

    @Test
    void deserializationWorks() {
        NetworkMessage message = commandWithRequiredCatalogVersion(42);

        byte[] serialized = partitionCommandsMarshaller.marshall(message);

        FinishTxCommand unmarshalled = partitionCommandsMarshaller.unmarshall(serialized);

        assertThat(unmarshalled.requiredCatalogVersion(), is(42));

        assertThat(partitionCommandsMarshaller.readRequiredCatalogVersion(ByteBuffer.wrap(serialized)), is(42));
    }
}
