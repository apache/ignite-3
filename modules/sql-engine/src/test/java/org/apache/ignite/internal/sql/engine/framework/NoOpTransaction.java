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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.PrimaryMetaTestImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy transaction that should be used as mock transaction for execution tests.
 */
public final class NoOpTransaction implements InternalTransaction {

    private final UUID id = UUID.randomUUID();

    private final HybridTimestamp hybridTimestamp = new HybridTimestamp(1, 1);

    private final ReplicaMeta replica;

    private final TablePartitionId groupId = new TablePartitionId(1, 0);

    private final boolean readOnly;

    /** Creates a read-write transaction. */
    public static NoOpTransaction readWrite(String name) {
        return new NoOpTransaction(name, false);
    }

    /** Creates a read only transaction. */
    public static NoOpTransaction readOnly(String name) {
        return new NoOpTransaction(name, true);
    }

    /**
     * Constructs a read only transaction.
     *
     * @param name Name of the node.
     */
    public NoOpTransaction(String name) {
        this(name, true);
    }

    /**
     * Constructs a transaction.
     *
     * @param name Name of the node.
     * @param readOnly Read-only or not.
     */
    private NoOpTransaction(String name, boolean readOnly) {
        var networkAddress = NetworkAddress.from(new InetSocketAddress("localhost", 1234));
        this.tuple = new IgniteBiTuple<>(new ClusterNode(name, name, networkAddress), 1L);
        this.readOnly = readOnly;
        this.replica = new PrimaryMetaTestImpl(name, new HybridTimestamp(1L, 0), HybridTimestamp.MAX_VALUE);
    }

    /** Node at which this transaction was start. */
    public ClusterNode clusterNode() {
        return tuple.get1();
    }

    @Override
    public void commit() throws TransactionException {

    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void rollback() throws TransactionException {

    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return CompletableFuture.completedFuture(null);
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
    public @NotNull UUID id() {
        return id;
    }

    @Override
    public ReplicaMeta enlistedReplica(TablePartitionId tablePartitionId) {
        return replica;
    }

    @Override
    public TxState state() {
        return TxState.COMMITED;
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
    public ReplicaMeta enlist(TablePartitionId tablePartitionId, ReplicaMeta replica) {
        return replica;
    }

    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        resultFuture.complete(null);
    }
}
