/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Fake Ignite.
 */
public class FakeIgnite implements Ignite {
    /**
     * Default constructor.
     */
    public FakeIgnite() {
        super();
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
            public IgniteTransactions withTimeout(long timeout) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Transaction begin() {
                return beginAsync().join();
            }

            @Override
            public CompletableFuture<Transaction> beginAsync() {
                return CompletableFuture.completedFuture(new Transaction() {
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
        return null;
    }
}
