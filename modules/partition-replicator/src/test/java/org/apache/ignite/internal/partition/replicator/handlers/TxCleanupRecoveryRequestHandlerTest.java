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

package org.apache.ignite.internal.partition.replicator.handlers;

import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxCleanupRecoveryRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageClosedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageDestroyedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageException;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TxCleanupRecoveryRequestHandlerTest extends BaseIgniteAbstractTest {
    private final TxMessagesFactory txMessagesFactory = new TxMessagesFactory();
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    private final ReplicationGroupId replicationGroupId = new ZonePartitionId(0, 0);

    @Mock
    private TxStatePartitionStorage txStatePartitionStorage;

    @Mock
    private TxManager txManager;

    @Mock
    private FailureProcessor failureProcessor;

    private TxCleanupRecoveryRequestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TxCleanupRecoveryRequestHandler(txStatePartitionStorage, txManager, failureProcessor, replicationGroupId);
    }

    @Test
    void interruptsCleanupGracefullyOnStorageClose() {
        testInterruptsCleanupGracefullyOnStorageClosed(TxStateStorageClosedException::new);
    }

    @Test
    void interruptsCleanupGracefullyOnStorageDestruction() {
        testInterruptsCleanupGracefullyOnStorageClosed(TxStateStorageDestroyedException::new);
    }

    @SuppressWarnings("unchecked")
    private void testInterruptsCleanupGracefullyOnStorageClosed(Supplier<? extends TxStateStorageException> exceptionFactory) {
        Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = mock(Cursor.class);
        Iterator<IgniteBiTuple<UUID, TxMeta>> iterator = mock(Iterator.class);

        when(txStatePartitionStorage.scan()).thenReturn(cursor);
        when(cursor.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenThrow(exceptionFactory.get());

        TxCleanupRecoveryRequest request = txMessagesFactory.txCleanupRecoveryRequest()
                .groupId(toReplicationGroupIdMessage(replicaMessagesFactory, replicationGroupId))
                .build();
        assertThat(handler.handle(request), willCompleteSuccessfully());

        verify(failureProcessor, never()).process(any());
    }
}
