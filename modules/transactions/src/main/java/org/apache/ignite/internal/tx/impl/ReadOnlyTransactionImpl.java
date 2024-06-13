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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.network.ClusterNode;

/**
 * The read-only implementation of an internal transaction.
 */
class ReadOnlyTransactionImpl extends IgniteAbstractTransactionImpl {
    /** The read timestamp. */
    private final HybridTimestamp readTimestamp;

    /** Prevents double finish of the transaction. */
    private final AtomicBoolean finishGuard = new AtomicBoolean();

    /** The tracker is used to track an observable timestamp. */
    private final HybridTimestampTracker observableTsTracker;

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param observableTsTracker Observable timestamp tracker.
     * @param id The id.
     * @param txCoordinatorId Transaction coordinator inconsistent ID.
     * @param readTimestamp The read timestamp.
     */
    ReadOnlyTransactionImpl(
            TxManagerImpl txManager,
            HybridTimestampTracker observableTsTracker,
            UUID id,
            String txCoordinatorId,
            HybridTimestamp readTimestamp
    ) {
        super(txManager, id, txCoordinatorId);

        this.readTimestamp = readTimestamp;
        this.observableTsTracker = observableTsTracker;
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
    public IgniteBiTuple<ClusterNode, Long> enlist(
            ZonePartitionId zonePartitionId,
            IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken
    ) {
        return null;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(ZonePartitionId zonePartitionId) {
        return null;
    }

    @Override
    public boolean assignCommitPartition(ZonePartitionId zonePartitionId) {
        return true;
    }

    @Override
    public ZonePartitionId zoneCommitPartition() {
        return null;
    }

    @Override
    protected CompletableFuture<Void> finish(boolean commit) {
        return finish(commit, readTimestamp);
    }

    @Override
    public CompletableFuture<Void> finish(boolean commit, HybridTimestamp executionTimestamp) {
        if (!finishGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        observableTsTracker.update(executionTimestamp);

        return ((TxManagerImpl) txManager).completeReadOnlyTransactionFuture(new TxIdAndTimestamp(readTimestamp, id()));
    }
}
