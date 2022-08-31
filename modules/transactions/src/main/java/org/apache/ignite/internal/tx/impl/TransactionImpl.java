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

package org.apache.ignite.internal.tx.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
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

    private volatile List<CompletableFuture>  enlistedResults = new ArrayList<>();

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
    public IgniteBiTuple<ClusterNode, Long> enlist(String repicationGroupId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        enlisted.put(repicationGroupId, nodeAndTerm);

        return nodeAndTerm;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        try {
            commitAsync().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            } else {
                throw new TransactionException(e.getCause());
            }
        } catch (Exception e) {
            throw new TransactionException(e);
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
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            } else {
                throw new TransactionException(e.getCause());
            }
        } catch (Exception e) {
            throw new TransactionException(e);
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
        TreeMap<ClusterNode, List<String>> groups = new TreeMap<>();

        // TODO: sanpwc better conversion required.
        enlisted.forEach((groupId, groupMeta) -> {
            ClusterNode recipientNode = groupMeta.get1();

            if (groups.containsKey(recipientNode)) {
                groups.get(recipientNode).add(groupId);
            } else {
                groups.put(recipientNode, new ArrayList<>()).add(groupId);
            }
        });

        return CompletableFuture.allOf(enlistedResults.toArray(new CompletableFuture[0])).handle(
                (ignored, ex) -> {
                    if (ex != null && commit) {
                        throw new TransactionException("Unable to commit the transaction with partially failed operations.");
                    } else {
                        if (!enlisted.isEmpty()) {
                            txManager.finish(
                                    enlisted.entrySet().iterator().next().getValue().get1(),
                                    commit,
                                    groups,
                                    id
                            );
                        }

                        return null;
                    }
                }
        );
        // TODO: sanpwc add debug log.
    }

    /** {@inheritDoc} */
    @Override
    public void enlistResultFuture(CompletableFuture resultFuture) {
        enlistedResults.add(resultFuture);
    }
}
