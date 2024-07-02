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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Implements the logic of persistent tx states vacuum.
 */
public class PersistentTxStateVacuumizer {
    private static final IgniteLogger LOG = Loggers.forClass(PersistentTxStateVacuumizer.class);

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final ReplicaService replicaService;

    private final ClusterNode localNode;

    private final ClockService clockService;

    private final PlacementDriver placementDriver;

    /**
     * Constructor.
     *
     * @param replicaService Replica service.
     * @param localNode Local node.
     * @param clockService Clock service.
     * @param placementDriver Placement driver.
     */
    public PersistentTxStateVacuumizer(
            ReplicaService replicaService,
            ClusterNode localNode,
            ClockService clockService,
            PlacementDriver placementDriver
    ) {
        this.replicaService = replicaService;
        this.localNode = localNode;
        this.clockService = clockService;
        this.placementDriver = placementDriver;
    }

    /**
     * Vacuum persistent tx states.
     *
     * @param txIds Transaction ids to vacuum; map of commit partition ids to sets of {@link VacuumizableTx}.
     * @return A future, result is the set of successfully processed txn states and count of persistent states that were vacuumized.
     */
    public CompletableFuture<PersistentTxStateVacuumResult> vacuumPersistentTxStates(Map<TablePartitionId, Set<VacuumizableTx>> txIds) {
        Set<UUID> successful = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicInteger vacuumizedPersistentTxnStatesCount = new AtomicInteger();
        HybridTimestamp now = clockService.now();

        txIds.forEach((commitPartitionId, txs) -> {
            CompletableFuture<?> future = placementDriver.getPrimaryReplica(commitPartitionId, now)
                    .thenCompose(replicaMeta -> {
                        // If the primary replica is absent or is not located on the local node, this means that the primary either is
                        // on another node or would be re-elected on local one; then the volatile state (as well as cleanup completion
                        // timestamp) would be updated there, and then this operation would be called from there.
                        // Also, we are going to send the vacuum request only to the local node.
                        if (replicaMeta != null && localNode.id().equals(replicaMeta.getLeaseholderId())) {
                            // Filter txns to define those which persistent states can be vacuumized.
                            Set<UUID> filteredTxIds = new HashSet<>();

                            for (VacuumizableTx v : txs) {
                                if (v.cleanupCompletionTimestamp == null) {
                                    // If there is no cleanup completion timestamp, add the tx id to successful set to remove the
                                    // volatile state. Persistent state should be preserved until tx cleanup is complete.
                                    successful.add(v.txId);
                                } else {
                                    filteredTxIds.add(v.txId);
                                }
                            }

                            VacuumTxStateReplicaRequest request = TX_MESSAGES_FACTORY.vacuumTxStateReplicaRequest()
                                    .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                                    .groupId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartitionId))
                                    .transactionIds(filteredTxIds)
                                    .build();

                            return replicaService.invoke(localNode, request).whenComplete((v, e) -> {
                                if (e == null) {
                                    successful.addAll(filteredTxIds);
                                    vacuumizedPersistentTxnStatesCount.addAndGet(filteredTxIds.size());
                                    // We can log the exceptions without further handling because failed requests' txns are not added
                                    // to the set of successful and will be retried. PrimaryReplicaMissException can be considered as
                                    // a part of regular flow and doesn't need to be logged.
                                } else if (unwrapCause(e) instanceof PrimaryReplicaMissException) {
                                    LOG.debug("Failed to vacuum tx states from the persistent storage.", e);
                                } else {
                                    LOG.warn("Failed to vacuum tx states from the persistent storage.", e);
                                }
                            });
                        } else {
                            successful.addAll(txs.stream().map(v -> v.txId).collect(toSet()));

                            return nullCompletedFuture();
                        }
                    });

            futures.add(future);
        });

        return allOf(futures)
                .handle((unused, unusedEx) -> new PersistentTxStateVacuumResult(successful, vacuumizedPersistentTxnStatesCount.get()));
    }

    /**
     * Class representing the vaccumizable tx context, actually a pair of the tx id and the cleanup completion timestamp.
     */
    public static class VacuumizableTx {
        final UUID txId;

        @Nullable
        final Long cleanupCompletionTimestamp;

        VacuumizableTx(UUID txId, @Nullable Long cleanupCompletionTimestamp) {
            this.txId = txId;
            this.cleanupCompletionTimestamp = cleanupCompletionTimestamp;
        }
    }

    /**
     * Result of the persistent tx state vacuum operation.
     */
    public static class PersistentTxStateVacuumResult {
        /** Transaction IDs that are processed by the persistent vacuumizer and can be removed from volatile storage. */
        final Set<UUID> txnsToVacuum;

        final int vacuumizedPersistentTxnStatesCount;

        public PersistentTxStateVacuumResult(Set<UUID> txnsToVacuum, int vacuumizedPersistentTxnStatesCount) {
            this.txnsToVacuum = txnsToVacuum;
            this.vacuumizedPersistentTxnStatesCount = vacuumizedPersistentTxnStatesCount;
        }
    }
}
