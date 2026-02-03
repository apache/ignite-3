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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.raft.FinishTxCommandResult;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeInternalException;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.PartitionEnlistmentMessage;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TxFinishReplicaRequestHandlerTest extends BaseIgniteAbstractTest {
    private static final long ANY_ENLISTMENT_CONSISTENCY_TOKEN = 1L;

    private final TxMessagesFactory txMessagesFactory = new TxMessagesFactory();
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    private final ZonePartitionId replicationGroupId = new ZonePartitionId(1, 1);

    @Mock
    private TxStatePartitionStorage txStatePartitionStorage;

    @Mock
    private ClockService clockService;

    @Mock
    private TxManager txManager;

    @Mock
    private ValidationSchemasSource validationSchemasSource;

    @Mock
    private SchemaSyncService schemaSyncService;

    @Mock
    private CatalogService catalogService;

    @Mock
    private RaftCommandRunner raftCommandRunner;

    private TxFinishReplicaRequestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TxFinishReplicaRequestHandler(
                txStatePartitionStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                raftCommandRunner,
                replicationGroupId
        );
    }

    @Test
    void finishReturnsMismatchingOutcomeAfterRaftWhenCommandNotApplied() {
        UUID txId = UUID.randomUUID();
        HybridTimestamp now = new HybridTimestamp(1, 1);

        when(clockService.now()).thenReturn(now);
        when(schemaSyncService.waitForMetadataCompleteness(any())).thenReturn(completedFuture(null));
        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(1);
        when(txStatePartitionStorage.get(txId)).thenReturn(null);

        TransactionResult committedResult = new TransactionResult(TxState.COMMITTED, now);
        FinishTxCommandResult finishResult = new FinishTxCommandResult(false, committedResult);

        when(raftCommandRunner.run(any())).thenReturn(completedFuture(finishResult));

        TxFinishReplicaRequest request = txMessagesFactory.txFinishReplicaRequest()
                .groupId(toZonePartitionIdMessage(replicaMessagesFactory, replicationGroupId))
                .commitPartitionId(toZonePartitionIdMessage(replicaMessagesFactory, replicationGroupId))
                .txId(txId)
                .groups(Map.of(
                        toZonePartitionIdMessage(replicaMessagesFactory, replicationGroupId),
                        partitionEnlistmentMessage("node", Set.of(1))
                ))
                .commit(false)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();

        assertThat(handler.handle(request), willThrow(MismatchingTransactionOutcomeInternalException.class));

        verify(txManager, never()).cleanup(any(), any(Map.class), anyBoolean(), any(), any());
    }

    private PartitionEnlistmentMessage partitionEnlistmentMessage(String primaryConsistentId, Set<Integer> tableIds) {
        return txMessagesFactory.partitionEnlistmentMessage()
                .primaryConsistentId(primaryConsistentId)
                .tableIds(tableIds)
                .build();
    }
}

