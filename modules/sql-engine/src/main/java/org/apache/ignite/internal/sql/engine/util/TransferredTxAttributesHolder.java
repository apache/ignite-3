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

package org.apache.ignite.internal.sql.engine.util;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Holder of {@link InternalTransaction#id() id} and {@link InternalTransaction#readTimestamp() readTimestamp} transaction attributes
 * that are passed to remote nodes during sql query execution.
 */
// TODO IGNITE-17952 This class must not implement the transaction interface.
public class TransferredTxAttributesHolder implements InternalTransaction {
    /** Transaction id. */
    private final UUID id;

    /** Read timestamp. */
    private final @Nullable HybridTimestamp readTimestamp;

    /**
     * Constructor.
     *
     * @param id Transaction id.
     * @param readTimestamp Read timestamp.
     */
    public TransferredTxAttributesHolder(UUID id, @Nullable HybridTimestamp readTimestamp) {
        this.readTimestamp = readTimestamp;
        this.id = id;
    }

    @Override
    public boolean isReadOnly() {
        return readTimestamp != null;
    }

    @Override
    public @Nullable HybridTimestamp readTimestamp() {
        return readTimestamp;
    }

    @Override
    public @NotNull UUID id() {
        return id;
    }

    @Override
    public void commit() throws TransactionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws TransactionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(ReplicationGroupId replicationGroupId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TxState state() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReplicationGroupId commitPartition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(ReplicationGroupId replicationGroupId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        throw new UnsupportedOperationException();
    }
}
