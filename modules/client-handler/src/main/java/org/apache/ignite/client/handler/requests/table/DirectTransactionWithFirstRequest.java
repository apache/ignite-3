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

package org.apache.ignite.client.handler.requests.table;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

class DirectTransactionWithFirstRequest implements InternalTransaction, Wrapper {
    private final InternalTransaction base;

    // We could also just accept a lambda.
    private final Map<Long, Long> reqToTxMap;

    private final long firstReqId;

    DirectTransactionWithFirstRequest(InternalTransaction base, Map<Long, Long> reqToTxMap, long firstReqId) {
        this.base = base;
        this.reqToTxMap = reqToTxMap;
        this.firstReqId = firstReqId;
    }

    @Override
    public UUID id() {
        return base.id();
    }

    @Override
    public PendingTxPartitionEnlistment enlistedPartition(ZonePartitionId replicationGroupId) {
        return base.enlistedPartition(replicationGroupId);
    }

    @Override
    public TxState state() {
        return base.state();
    }

    @Override
    public boolean assignCommitPartition(ZonePartitionId commitPartitionId) {
        return base.assignCommitPartition(commitPartitionId);
    }

    @Override
    public ZonePartitionId commitPartition() {
        return base.commitPartition();
    }

    @Override
    public void enlist(ZonePartitionId replicationGroupId, int tableId, String primaryNodeConsistentId, long consistencyToken) {
        base.enlist(replicationGroupId, tableId, primaryNodeConsistentId, consistencyToken);
    }

    @Override
    public @Nullable HybridTimestamp readTimestamp() {
        return base.readTimestamp();
    }

    @Override
    public HybridTimestamp schemaTimestamp() {
        return base.schemaTimestamp();
    }

    @Override
    public UUID coordinatorId() {
        return base.coordinatorId();
    }

    @Override
    public boolean implicit() {
        return base.implicit();
    }

    @Override
    public boolean remote() {
        return base.remote();
    }

    @Override
    public boolean remoteOnCoordinator() {
        return base.remoteOnCoordinator();
    }

    @Override
    public CompletableFuture<Void> finish(boolean commit, @Nullable HybridTimestamp executionTimestamp, boolean full,
            @Nullable Throwable finishReason) {
        return base.finish(commit, executionTimestamp, full, finishReason).whenComplete((v, err) -> removeMapping());
    }

    @Override
    public boolean isFinishingOrFinished() {
        return base.isFinishingOrFinished();
    }

    @Override
    public long getTimeout() {
        return base.getTimeout();
    }

    @Override
    public CompletableFuture<Void> kill() {
        return base.kill().whenComplete((v, err) -> removeMapping());
    }

    @Override
    public CompletableFuture<Void> rollbackWithExceptionAsync(Throwable throwable) {
        return base.rollbackWithExceptionAsync(throwable).whenComplete((v, err) -> removeMapping());
    }

    @Override
    public boolean isRolledBackWithTimeoutExceeded() {
        return base.isRolledBackWithTimeoutExceeded();
    }

    @Override
    public void processDelayedAck(Object val, @Nullable Throwable err) {
        base.processDelayedAck(val, err);
    }

    @Override
    public void commit() throws TransactionException {
        try {
            base.commit();
        } finally {
            removeMapping();
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return base.commitAsync().whenComplete((v, err) -> removeMapping());
    }

    @Override
    public void rollback() throws TransactionException {
        try {
            base.rollback();
        } finally {
            removeMapping();
        }
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return base.rollbackAsync().whenComplete((v, err) -> removeMapping());
    }

    @Override
    public boolean isReadOnly() {
        return base.isReadOnly();
    }

    public InternalTransaction base() {
        return base;
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return (T) base;
    }

    private void removeMapping() {
        reqToTxMap.remove(firstReqId);
    }
}
