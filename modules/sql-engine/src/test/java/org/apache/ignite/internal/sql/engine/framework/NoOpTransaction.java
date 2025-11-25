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

package org.apache.ignite.internal.sql.engine.framework;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;

/**
 * Dummy transaction that should be used as mock transaction for execution tests.
 */
public final class NoOpTransaction implements InternalTransaction {
    private static final int ZONE_ID = 1;

    private static final int TABLE_ID = 2;

    private static final int PARTITION_ID = 2;

    private final UUID id;

    private final HybridTimestamp hybridTimestamp = new HybridTimestamp(1, 1)
            .addPhysicalTime(System.currentTimeMillis());

    private final InternalClusterNode enlistmentNode;

    private final PendingTxPartitionEnlistment enlistment;

    private final ZonePartitionId groupId = new ZonePartitionId(ZONE_ID, PARTITION_ID);

    private final boolean implicit;

    private final boolean readOnly;

    private boolean isRolledBackWithTimeoutExceeded = false;

    private final CompletableFuture<Void> commitFut = new CompletableFuture<>();

    private final CompletableFuture<Void> rollbackFut = new CompletableFuture<>();

    /** Creates a read-write transaction. */
    public static NoOpTransaction readWrite(String name, boolean implicit) {
        return new NoOpTransaction(name, implicit, false);
    }

    /** Creates a read only transaction. */
    public static NoOpTransaction readOnly(String name, boolean implicit) {
        return new NoOpTransaction(name, implicit, true);
    }

    /**
     * Constructs a read only transaction.
     *
     * @param name Name of the node.
     */
    public NoOpTransaction(String name, boolean implicit) {
        this(name, implicit, true);
    }

    /**
     * Constructs a transaction.
     *
     * @param name Name of the node.
     * @param implicit Implicit transaction flag.
     * @param readOnly Read-only or not.
     */
    public NoOpTransaction(String name, boolean implicit, boolean readOnly) {
        var networkAddress = NetworkAddress.from(new InetSocketAddress("localhost", 1234));
        this.enlistmentNode = new ClusterNodeImpl(randomUUID(), name, networkAddress);
        this.enlistment = new PendingTxPartitionEnlistment(enlistmentNode.name(), 1L, TABLE_ID);
        this.implicit = implicit;
        this.readOnly = readOnly;

        this.id = readOnly ?  randomUUID() : TransactionIds.transactionId(hybridTimestamp, enlistmentNode.name().hashCode());
    }

    /** Node at which this transaction was start. */
    public InternalClusterNode clusterNode() {
        return enlistmentNode;
    }

    @Override
    public void commit() throws TransactionException {
        commitAsync().join();
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return finish(true, nullableHybridTimestamp(NULL_HYBRID_TIMESTAMP), false, false);
    }

    @Override
    public void rollback() throws TransactionException {
        rollbackAsync().join();
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return finish(false, nullableHybridTimestamp(NULL_HYBRID_TIMESTAMP), false, false);
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public HybridTimestamp readTimestamp() {
        if (!isReadOnly()) {
            return null;
        }
        return hybridTimestamp;
    }

    @Override
    public HybridTimestamp schemaTimestamp() {
        return hybridTimestamp;
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public UUID coordinatorId() {
        return clusterNode().id();
    }

    @Override
    public PendingTxPartitionEnlistment enlistedPartition(ReplicationGroupId tablePartitionId) {
        return enlistment;
    }

    @Override
    public TxState state() {
        return TxState.COMMITTED;
    }

    @Override
    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
        return true;
    }

    @Override
    public ZonePartitionId commitPartition() {
        return groupId;
    }

    @Override
    public boolean implicit() {
        return implicit;
    }

    @Override
    public CompletableFuture<Void> finish(boolean commit, HybridTimestamp executionTimestamp, boolean full, boolean timeoutExceeded) {
        CompletableFuture<Void> fut = commit ? commitFut : rollbackFut;

        fut.complete(null);

        return fut;
    }

    @Override
    public boolean isFinishingOrFinished() {
        return commitFut.isDone() || rollbackFut.isDone();
    }

    @Override
    public long getTimeout() {
        return 10_000;
    }

    @Override
    public void enlist(
            ReplicationGroupId replicationGroupId,
            int tableId,
            String primaryNodeConsistentId,
            long consistencyToken
    ) {
        requireNonNull(replicationGroupId, "replicationGroupId");
        requireNonNull(primaryNodeConsistentId, "primaryNodeConsistentId");

        // No-op.
    }

    @Override
    public CompletableFuture<Void> kill() {
        return rollbackAsync();
    }

    @Override
    public CompletableFuture<Void> rollbackTimeoutExceededAsync() {
        this.isRolledBackWithTimeoutExceeded = true;
        return rollbackAsync();
    }

    @Override
    public boolean isRolledBackWithTimeoutExceeded() {
        return isRolledBackWithTimeoutExceeded;
    }

    /** Returns a {@link CompletableFuture} that completes when this transaction commits. */
    public CompletableFuture<Void> commitFuture() {
        return commitFut;
    }

    /** Returns a {@link CompletableFuture} that completes when this transaction rollbacks. */
    public CompletableFuture<Void> rollbackFuture() {
        return rollbackFut;
    }
}
