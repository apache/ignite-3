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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_MISS_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Remote read-write transaction which is coordinated from elsewhere.
 * Used for a direct mapping request from a client.
 */
public abstract class RemoteReadWriteTransaction implements InternalTransaction {
    private static final String EXCEPTION_MSG = "Remote transaction should never be finished directly";

    private final UUID txId;
    private final ZonePartitionId commitGroupId;
    private final long timeout;
    private final UUID coord;
    private final String localNodeConsistentId;
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
    RemoteReadWriteTransaction(UUID txId, ZonePartitionId commitGroupId, UUID coord, long token, InternalClusterNode localNode,
            long timeout) {
        this.txId = txId;
        this.commitGroupId = commitGroupId;
        this.coord = coord;
        this.timeout = timeout;
        this.localNodeConsistentId = localNode.name();
        this.enlistment = token == 0 ? null : new PendingTxPartitionEnlistment(localNodeConsistentId, token);
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
    public CompletableFuture<Void> rollbackWithExceptionAsync(Throwable throwable) {
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
    public PendingTxPartitionEnlistment enlistedPartition(ZonePartitionId replicationGroupId) {
        return enlistment;
    }

    @Override
    public boolean assignCommitPartition(ZonePartitionId replicationGroupId) {
        return false;
    }

    @Override
    public ZonePartitionId commitPartition() {
        return commitGroupId;
    }

    @Override
    public void enlist(ZonePartitionId replicationGroupId, int tableId, String primaryNodeConsistentId, long consistencyToken) {
        // Validate primary replica.
        if (!localNodeConsistentId.equals(primaryNodeConsistentId)) {
            throw new TransactionException(REPLICA_MISS_ERR, format("The primary replica has changed [txId={}, "
                            + "expectedPrimaryReplicaConsistentId={}, currentPrimaryReplicaConsistentId={}].", txId, localNodeConsistentId,
                    primaryNodeConsistentId));
        }

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
    public String toString() {
        return S.toString(RemoteReadWriteTransaction.class, this);
    }
}
