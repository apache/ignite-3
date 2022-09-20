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

import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The implementation of an internal transaction.
 *
 * <p>Delegates state management to tx manager.
 */
public class TransactionImpl implements InternalTransaction {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TransactionImpl.class);

    /** The id. */
    private final UUID id;

    /** The transaction manager. */
    private final TxManager txManager;

    /** Enlisted replication groups: replication group id -> (primary replica node, raft term). */
    private Map<String, IgniteBiTuple<ClusterNode, Long>> enlisted = new ConcurrentSkipListMap<>();

    /** Enlisted operation futures in this transaction. */
    private volatile List<CompletableFuture<?>>  enlistedResults = new ArrayList<>();

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param id The id.
     */
    public TransactionImpl(TxManager txManager, @NotNull UUID id) {
        this.txManager = txManager;
        this.id = id;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(String partGroupId) {
        return enlisted.get(partGroupId);
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public TxState state() {
        return txManager.state(id);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(String replicationGroupId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        enlisted.put(replicationGroupId, nodeAndTerm);

        return nodeAndTerm;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        try {
            commitAsync().get();
        } catch (Exception e) {
            throw withCause(TransactionException::new, TX_COMMIT_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync() {
        return finish(true);
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        try {
            rollbackAsync().get();
        } catch (Exception e) {
            throw withCause(TransactionException::new, TX_ROLLBACK_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return finish(false);
    }

    /**
     * Finishes a transaction.
     *
     * @param commit {@code true} to commit, false to rollback.
     * @return The future.
     */
    private CompletableFuture<Void> finish(boolean commit) {
        Map<ClusterNode, List<IgniteBiTuple<String, Long>>> groups = new LinkedHashMap<>();

        // TODO: sanpwc better conversion required.
        enlisted.forEach((groupId, groupMeta) -> {
            ClusterNode recipientNode = groupMeta.get1();

            if (groups.containsKey(recipientNode)) {
                groups.get(recipientNode).add(new IgniteBiTuple<>(groupId, groupMeta.get2()));
            } else {
                List<IgniteBiTuple<String, Long>> items = new ArrayList<>();

                items.add(new IgniteBiTuple<>(groupId, groupMeta.get2()));

                groups.put(recipientNode, items);
            }
        });

        // TODO: add proper exception handling.
        return CompletableFuture
                .allOf(enlistedResults.toArray(new CompletableFuture[0]))
                .thenCompose(
                        ignored -> {
                            if (!enlisted.isEmpty()) {
                                return txManager.finish(
                                        enlisted.entrySet().iterator().next().getValue().get1(),
                                        enlisted.entrySet().iterator().next().getValue().get2(),
                                        commit,
                                        groups,
                                        id
                                );
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                );
        // TODO: sanpwc add debug log.
    }

    /** {@inheritDoc} */
    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        enlistedResults.add(resultFuture);
    }
}
