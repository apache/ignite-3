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

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;

/**
 * Dummy transaction that should be used as mock transaction for execution tests.
 */
public final class NoOpTransaction implements InternalTransaction {

    private final UUID id = randomUUID();

    private final HybridTimestamp hybridTimestamp = new HybridTimestamp(1, 1)
            .addPhysicalTime(System.currentTimeMillis());

    private final IgniteBiTuple<ClusterNode, Long> tuple;

    private final TablePartitionId groupId = new TablePartitionId(1, 0);

    private final boolean implicit;

    private final boolean readOnly;

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
        this.tuple = new IgniteBiTuple<>(new ClusterNodeImpl(randomUUID(), name, networkAddress), 1L);
        this.implicit = implicit;
        this.readOnly = readOnly;
    }

    /** Node at which this transaction was start. */
    public ClusterNode clusterNode() {
        return tuple.get1();
    }

    @Override
    public void commit() throws TransactionException {
        commitAsync().join();
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return finish(true, nullableHybridTimestamp(NULL_HYBRID_TIMESTAMP), false);
    }

    @Override
    public void rollback() throws TransactionException {
        rollbackAsync().join();
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return finish(false, nullableHybridTimestamp(NULL_HYBRID_TIMESTAMP), false);
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
    public HybridTimestamp startTimestamp() {
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
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(TablePartitionId tablePartitionId) {
        return tuple;
    }

    @Override
    public TxState state() {
        return TxState.COMMITTED;
    }

    @Override
    public boolean assignCommitPartition(TablePartitionId tablePartitionId) {
        return true;
    }

    @Override
    public TablePartitionId commitPartition() {
        return groupId;
    }

    @Override
    public boolean implicit() {
        return implicit;
    }

    @Override
    public CompletableFuture<Void> finish(boolean commit, HybridTimestamp executionTimestamp, boolean full) {
        CompletableFuture<Void> fut = commit ? commitFut : rollbackFut;

        fut.complete(null);

        return fut;
    }

    @Override
    public boolean isFinishingOrFinished() {
        return commitFut.isDone() || rollbackFut.isDone();
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(TablePartitionId tablePartitionId,
            IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken) {
        return nodeAndConsistencyToken;
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
