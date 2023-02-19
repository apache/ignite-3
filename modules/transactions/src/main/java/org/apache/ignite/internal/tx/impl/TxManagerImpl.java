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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableLongTime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private ReplicaService replicaService;

    /** Lock manager. */
    private final LockManager lockManager;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    // TODO: IGNITE-17638 Consider using Txn state map instead of states.
    /** The storage for tx states. */
    @TestOnly
    private final ConcurrentHashMap<UUID, TxState> states = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     */
    public TxManagerImpl(ReplicaService replicaService, LockManager lockManager, HybridClock clock) {
        this.replicaService = replicaService;
        this.lockManager = lockManager;
        this.clock = clock;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTransaction begin() {
        return begin(false);
    }

    /** {@inheritDoc} */
    @Override
    public InternalTransaction begin(boolean readOnly) {
        UUID txId = Timestamp.nextVersion().toUuid();

        return readOnly ? new ReadOnlyTransactionImpl(this, txId, clock.now()) : new ReadWriteTransactionImpl(this, txId);
    }

    /** {@inheritDoc} */
    @Override
    public TxState state(UUID txId) {
        return states.get(txId);
    }

    /** {@inheritDoc} */
    @Override
    public boolean changeState(UUID txId, TxState before, TxState after) {
        return states.compute(txId, (k, v) -> {
            if (v == before) {
                return after;
            } else {
                return v;
            }
        }) == after;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> finish(
            ReplicationGroupId commitPartition,
            ClusterNode recipientNode,
            Long term,
            boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<ReplicationGroupId, Long>>> groups,
            UUID txId
    ) {
        assert groups != null && !groups.isEmpty();

        HybridTimestamp commitTimestamp = commit ? clock.now() : null;

        TxFinishReplicaRequest req = FACTORY.txFinishReplicaRequest()
                .txId(txId)
                .groupId(commitPartition)
                .groups(groups)
                .commit(commit)
                .commitTimestampLong(nullableLongTime(commitTimestamp))
                .term(term)
                .build();

        return replicaService.invoke(recipientNode, req)
                // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
                .thenRun(() -> changeState(txId, null, commit ? TxState.COMMITED : TxState.ABORTED));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> cleanup(
            ClusterNode recipientNode,
            List<IgniteBiTuple<ReplicationGroupId, Long>> replicationGroupIds,
            UUID txId,
            boolean commit,
            HybridTimestamp commitTimestamp
    ) {
        var cleanupFutures = new CompletableFuture[replicationGroupIds.size()];

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17582 Grouping replica requests.
        for (int i = 0; i < replicationGroupIds.size(); i++) {
            cleanupFutures[i] = replicaService.invoke(
                    recipientNode,
                    FACTORY.txCleanupReplicaRequest()
                            .groupId(replicationGroupIds.get(i).get1())
                            .txId(txId)
                            .commit(commit)
                            .commitTimestampLong(nullableLongTime(commitTimestamp))
                            .term(replicationGroupIds.get(i).get2())
                            .build()
            );
        }

        return allOf(cleanupFutures);
    }

    /** {@inheritDoc} */
    @Override
    public int finished() {
        return (int) states.entrySet().stream().filter(e -> e.getValue() == TxState.COMMITED || e.getValue() == TxState.ABORTED).count();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public LockManager lockManager() {
        return lockManager;
    }
}
