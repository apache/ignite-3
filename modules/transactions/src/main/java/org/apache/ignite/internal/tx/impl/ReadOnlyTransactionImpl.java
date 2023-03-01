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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * The read-only implementation of an internal transaction.
 */
public class ReadOnlyTransactionImpl extends IgniteAbstractTransactionImpl {
    /** The read timestamp. */
    private final HybridTimestamp readTimestamp;

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param id The id.
     * @param readTimestamp The read timestamp.
     */
    public ReadOnlyTransactionImpl(
            TxManager txManager,
            @NotNull UUID id,
            HybridTimestamp readTimestamp
    ) {
        super(txManager, id);
        this.readTimestamp = readTimestamp;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(ReplicationGroupId replicationGroupId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        // TODO: IGNITE-17666 Close cursor tx finish and do it on the first finish invocation only.
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(ReplicationGroupId replicationGroupId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public ReplicationGroupId commitPartition() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    protected CompletableFuture<Void> finish(boolean commit) {
        // TODO: IGNITE-17666 Close cursor tx finish and do it on the first finish invocation only.
        return CompletableFuture.completedFuture(null);
    }
}
