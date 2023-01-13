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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * The read-write implementation of an internal transaction.
 */
public class ReadWriteTransactionImpl extends IgniteAbstractTransactionImpl {
    private static final IgniteLogger LOG = Loggers.forClass(InternalTransaction.class);

    /** Enlisted replication groups: replication group id -> (primary replica node, raft term). */
    private final Map<ReplicationGroupId, IgniteBiTuple<ClusterNode, Long>> enlisted = new ConcurrentHashMap<>();

    /** Enlisted operation futures in this transaction. */
    private final List<CompletableFuture<?>> enlistedResults = new CopyOnWriteArrayList<>();

    /** Reference to the partition that stores the transaction state. */
    private final AtomicReference<ReplicationGroupId> commitPartitionRef = new AtomicReference<>();

    /** The future used on repeated commit/rollback. */
    private final AtomicReference<CompletableFuture<Void>> finishFut = new AtomicReference<>();

    /** {@code true} if commit is started. */
    private volatile boolean commitState;

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param id The id.
     */
    public ReadWriteTransactionImpl(TxManager txManager, @NotNull UUID id) {
        super(txManager, id);
    }

    /** {@inheritDoc} */
    @Override
    public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
        return commitPartitionRef.compareAndSet(null, replicationGroupId);
    }

    /** {@inheritDoc} */
    @Override
    public ReplicationGroupId commitPartition() {
        return commitPartitionRef.get();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(ReplicationGroupId partGroupId) {
        return enlisted.get(partGroupId);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(ReplicationGroupId replicationGroupId, IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
        return enlisted.computeIfAbsent(replicationGroupId, k -> nodeAndTerm);
    }

    /** {@inheritDoc} */
    @Override
    protected CompletableFuture<Void> finish(boolean commit) {
        if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
            if (commitState && commit) {
                return finishFut.get();
            } else {
                return completedFuture(null);
            }
        }

        commitState = commit;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17688 Add proper exception handling.
        return CompletableFuture
                .allOf(enlistedResults.toArray(new CompletableFuture[0]))
                .thenCompose(
                        ignored -> {
                            if (!enlisted.isEmpty()) {
                                Map<ClusterNode, List<IgniteBiTuple<ReplicationGroupId, Long>>> groups = new LinkedHashMap<>();

                                enlisted.forEach((groupId, groupMeta) -> {
                                    ClusterNode recipientNode = groupMeta.get1();

                                    if (groups.containsKey(recipientNode)) {
                                        groups.get(recipientNode).add(new IgniteBiTuple<>(groupId, groupMeta.get2()));
                                    } else {
                                        List<IgniteBiTuple<ReplicationGroupId, Long>> items = new ArrayList<>();

                                        items.add(new IgniteBiTuple<>(groupId, groupMeta.get2()));

                                        groups.put(recipientNode, items);
                                    }
                                });

                                ReplicationGroupId commitPart = commitPartitionRef.get();
                                ClusterNode recipientNode = enlisted.get(commitPart).get1();
                                Long term = enlisted.get(commitPart).get2();

                                LOG.debug("Finish [recipientNode={}, term={} commit={}, txId={}, groups={}",
                                        recipientNode, term, commit, id(), groups);

                                return txManager.finish(
                                        commitPart,
                                        recipientNode,
                                        term,
                                        commit,
                                        groups,
                                        id()
                                );
                            } else {
                                return completedFuture(null);
                            }
                        }
                ).thenRun(() -> finishFut.get().complete(null));
    }

    /** {@inheritDoc} */
    @Override
    public void enlistResultFuture(CompletableFuture<?> resultFuture) {
        enlistedResults.add(resultFuture);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp readTimestamp() {
        return null;
    }
}
