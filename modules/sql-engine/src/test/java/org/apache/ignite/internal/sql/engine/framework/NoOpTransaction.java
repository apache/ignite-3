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

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy transaction that should be used as mock transaction for execution tests.
 */
public final class NoOpTransaction implements InternalTransaction {

    private final UUID id = UUID.randomUUID();

    private final HybridTimestamp hybridTimestamp = new HybridTimestamp(1, 1);

    private final IgniteBiTuple<ClusterNode, Long> tuple;

    private final ReplicationGroupId groupId = new ReplicationGroupId() {

        private static final long serialVersionUID = -6498147568339477517L;
    };

    /**
     * Constructs the object.
     *
     * @param name Name of the node.
     */
    public NoOpTransaction(String name) {
        var networkAddress = NetworkAddress.from(new InetSocketAddress("localhost", 1234));
        tuple = new IgniteBiTuple<>(new ClusterNode(name, name, networkAddress), 1L);
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
        return true;
    }

    @Override
    public HybridTimestamp readTimestamp() {
        return hybridTimestamp;
    }

    @Override
    public @NotNull UUID id() {
        return id;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(ReplicationGroupId replicationGroupId) {
        return tuple;
    }

    @Override
    public TxState state() {
        return TxState.COMMITED;
    }

    @Override
    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
        return true;
    }

    @Override
    public ReplicationGroupId commitPartition() {
        return groupId;
    }

    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(ReplicationGroupId replicationGroupId,
            IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        return nodeAndTerm;
    }

    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        resultFuture.complete(null);
    }
}
