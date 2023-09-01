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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.tx.TxState.PENDING;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;

/**
 * The read-only implementation of an internal transaction.
 */
class ReadOnlyTransactionImpl extends IgniteAbstractTransactionImpl {
    /** The read timestamp. */
    private final HybridTimestamp readTimestamp;

    /** Prevents double finish of the transaction. */
    private final AtomicBoolean finishGuard = new AtomicBoolean();

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param id The id.
     * @param readTimestamp The read timestamp.
     */
    ReadOnlyTransactionImpl(TxManagerImpl txManager, UUID id, HybridTimestamp readTimestamp) {
        super(txManager, id);

        this.readTimestamp = readTimestamp;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public HybridTimestamp readTimestamp() {
        return readTimestamp;
    }

    @Override
    public HybridTimestamp startTimestamp() {
        return readTimestamp;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(TablePartitionId tablePartitionId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        // TODO: IGNITE-17666 Close cursor tx finish and do it on the first finish invocation only.
        return null;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(TablePartitionId tablePartitionId) {
        return null;
    }

    @Override
    public boolean assignCommitPartition(TablePartitionId tablePartitionId) {
        return true;
    }

    @Override
    public TablePartitionId commitPartition() {
        return null;
    }

    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        // No-op.
    }

    @WithSpan
    @Override
    // TODO: IGNITE-17666 Close cursor tx finish and do it on the first finish invocation only.
    protected CompletableFuture<Void> finish(boolean commit) {
        if (!finishGuard.compareAndSet(false, true)) {
            return completedFuture(null);
        }

        return ((TxManagerImpl) txManager).completeReadOnlyTransactionFuture(
                new TxIdAndTimestamp(readTimestamp, id())).thenRun(() -> txManager.changeState(id(), PENDING, COMMITED));
    }
}
