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

package org.apache.ignite.client.fakes;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.deployment.IgniteDeployment;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.NotNull;

/**
 * Fake Ignite.
 */
public class FakeIgnite implements Ignite {
    private final String name;

    /**
     * Default constructor.
     */
    public FakeIgnite() {
        this(null);
    }

    /**
     * Constructor.
     *
     * @param name Name.
     */
    public FakeIgnite(String name) {
        super();
        this.name = name;
    }

    private final IgniteTables tables = new FakeIgniteTables();

    /** {@inheritDoc} */
    @Override
    public IgniteTables tables() {
        return tables;
    }

    public QueryProcessor queryEngine() {
        return new FakeIgniteQueryProcessor();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions transactions() {
        return new IgniteTransactions() {
            @Override
            public Transaction begin(TransactionOptions options) {
                return beginAsync(options).join();
            }

            @Override
            public CompletableFuture<Transaction> beginAsync(TransactionOptions options) {
                return CompletableFuture.completedFuture(new InternalTransaction() {
                    private final UUID id = UUID.randomUUID();

                    @Override
                    public @NotNull UUID id() {
                        return id;
                    }

                    @Override
                    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(ReplicationGroupId replicationGroupId) {
                        return null;
                    }

                    @Override
                    public TxState state() {
                        return null;
                    }

                    @Override
                    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
                        return false;
                    }

                    @Override
                    public ReplicationGroupId commitPartition() {
                        return null;
                    }

                    @Override
                    public IgniteBiTuple<ClusterNode, Long> enlist(
                            ReplicationGroupId replicationGroupId,
                            IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
                        return null;
                    }

                    @Override
                    public void enlistResultFuture(CompletableFuture<?> resultFuture) {}

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
                        return false;
                    }

                    @Override
                    public HybridTimestamp readTimestamp() {
                        return null;
                    }
                });
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSql sql() {
        return new FakeIgniteSql();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public IgniteDeployment deployment() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> clusterNodes() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNode>> clusterNodesAsync() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }
}
