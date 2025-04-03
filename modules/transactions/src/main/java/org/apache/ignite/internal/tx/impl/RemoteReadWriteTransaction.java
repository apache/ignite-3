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
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Remote read-write transaction which is coordinated from elsewhere.
 * Used for a direct mapping request from a client.
 */
public abstract class RemoteReadWriteTransaction implements InternalTransaction {
    private static final String EXCEPTION_MSG = "Remote transaction should never be finished directly";

    private final UUID txId;
    private final TablePartitionId commitGroupId;
    private final long timeout;
    private final UUID coord;
    @IgniteToStringExclude
    private @Nullable PendingTxPartitionEnlistment enlistment;

    /**
     * The constructor.
     *
     * @param txId Tx id.
     * @param commitGroupId Commit group id.
     * @param coord The coordinator id.
     * @param token Enlistment token.
     * @param localNode Local node.
     * @param timeout The timeout.
     */
    RemoteReadWriteTransaction(UUID txId, TablePartitionId commitGroupId, UUID coord, long token, ClusterNode localNode,
            long timeout) {
        this.txId = txId;
        this.commitGroupId = commitGroupId;
        this.coord = coord;
        this.timeout = timeout;
        this.enlistment = token == 0 ? null : new PendingTxPartitionEnlistment(localNode.name(), token);
    }

    @Override
    public void commit() throws TransactionException {
        throw new AssertionError(EXCEPTION_MSG);
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        throw new AssertionError(EXCEPTION_MSG);
    }

    @Override
    public void rollback() throws TransactionException {
        throw new AssertionError(EXCEPTION_MSG);
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        throw new AssertionError(EXCEPTION_MSG);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public UUID id() {
        return txId;
    }

    @Override
    public PendingTxPartitionEnlistment enlistedPartition(ReplicationGroupId replicationGroupId) {
        return enlistment;
    }

    @Override
    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
        return false;
    }

    @Override
    public TablePartitionId commitPartition() {
        return commitGroupId;
    }

    @Override
    public void enlist(ReplicationGroupId replicationGroupId, int tableId, String primaryNodeConsistentId, long consistencyToken) {
        this.enlistment = new PendingTxPartitionEnlistment(primaryNodeConsistentId, consistencyToken, tableId);
    }

    @Override
    public @Nullable HybridTimestamp readTimestamp() {
        return null;
    }

    @Override
    public HybridTimestamp schemaTimestamp() {
        return TransactionIds.beginTimestamp(txId);
    }

    @Override
    public UUID coordinatorId() {
        return coord;
    }

    @Override
    public boolean implicit() {
        return false;
    }

    @Override
    public boolean remote() {
        return true;
    }

    @Override
    public CompletableFuture<Void> finish(boolean commit, @Nullable HybridTimestamp executionTimestamp, boolean full,
            boolean timeoutExceeded) {
        return null;
    }

    @Override
    public boolean isFinishingOrFinished() {
        return false;
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public CompletableFuture<Void> kill() {
        return null;
    }

    @Override
    public String toString() {
        return S.toString(RemoteReadWriteTransaction.class, this);
    }
}
