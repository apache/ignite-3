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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.index.IndexManagementUtils.AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC;
import static org.apache.ignite.internal.index.IndexManagementUtils.earliestCatalogVersionOfIndexInRegisteredStatus;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.index.IndexManagementUtils.localNode;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.index.message.IndexMessagesFactory;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeRequest;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeResponse;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.RecipientLeftException;

/** Пока нет документации. */
// TODO: IGNITE-21115 код, тесты и документация
class IndexBuildingStarterTask {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildingStarterTask.class);

    private static final IndexMessagesFactory FACTORY = new IndexMessagesFactory();

    private static final int INVOKE_MESSAGE_TIMEOUT_MILLS = 1_000;

    private static final int RETRY_SEND_MESSAGE_TIMEOUT_MILLS = 100;

    private final CatalogIndexDescriptor indexDescriptor;

    private final CatalogManager catalogManager;

    private final PlacementDriver placementDriver;

    private final ClusterService clusterService;

    private final LogicalTopologyService logicalTopologyService;

    private final HybridClock clock;

    private final ClockWaiter clockWaiter;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock;

    private volatile int catalogVersionOfIndexCreation = -1;

    private final IgniteSpinBusyLock taskBusyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean taskStopGuard = new AtomicBoolean();

    IndexBuildingStarterTask(
            CatalogIndexDescriptor indexDescriptor,
            CatalogManager catalogManager,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            HybridClock clock,
            ClockWaiter clockWaiter,
            Executor executor,
            IgniteSpinBusyLock busyLock
    ) {
        this.indexDescriptor = indexDescriptor;
        this.catalogManager = catalogManager;
        this.placementDriver = placementDriver;
        this.clusterService = clusterService;
        this.logicalTopologyService = logicalTopologyService;
        this.clockWaiter = clockWaiter;
        this.clock = clock;
        this.executor = executor;
        this.busyLock = busyLock;
    }

    /**
     * Asynchronously starts the task execution.
     *
     * @return Future of task execution.
     */
    CompletableFuture<Void> start() {
        if (!enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return supplyAsync(() -> {
                int catalogVersionOfIndexCreation = earliestCatalogVersionOfIndexInRegisteredStatus(catalogManager, indexDescriptor.id());

                assert catalogVersionOfIndexCreation >= 0 : indexDescriptor;

                this.catalogVersionOfIndexCreation = catalogVersionOfIndexCreation;

                return awaitActivateForCatalogVersionOfIndexCreation()
                        .thenCompose(unused -> ensureThatLocalNodeStillPrimaryReplica())
                        .thenCompose(unused -> inBusyLock(logicalTopologyService::logicalTopologyOnLeader))
                        .thenComposeAsync(this::awaitFinishRwTxsBeforeCatalogVersionOfIndexCreation, executor)
                        .thenComposeAsync(unused -> switchIndexToBuildingStatus(), executor);
            }, executor)
                    .thenCompose(Function.identity())
                    .whenComplete((unused, throwable) -> {
                        if (throwable == null) {
                            return;
                        }

                        Throwable cause = unwrapCause(throwable);

                        if (!(cause instanceof TakStoppingException)) {
                            LOG.error("Error starting index building: {}", cause, indexDescriptor);
                        }
                    });
        } finally {
            leaveBusy();
        }
    }

    /** Stops the execution of a task. */
    void stop() {
        if (!taskStopGuard.compareAndSet(false, true)) {
            return;
        }

        taskBusyLock.block();
    }

    private CompletableFuture<Void> awaitActivateForCatalogVersionOfIndexCreation() {
        return inBusyLock(() -> {
            Catalog catalog = catalogManager.catalog(catalogVersionOfIndexCreation);

            assert catalog != null : IgniteStringFormatter.format("Missing catalog version: [index={}, catalogVersion={}]",
                    indexDescriptor, catalogVersionOfIndexCreation);

            return clockWaiter.waitFor(clusterWideEnsuredActivationTimestamp(catalog));
        });
    }

    private CompletableFuture<Void> ensureThatLocalNodeStillPrimaryReplica() {
        return awaitPrimaryReplica().thenAccept(replicaMeta -> {
            if (!enterBusy()) {
                throw new TakStoppingException();
            }

            try {
                if (!isPrimaryReplica(replicaMeta, localNode(clusterService), clock.now())) {
                    // Lease has expired, we stop the task.
                    throw new TakStoppingException();
                }
            } finally {
                leaveBusy();
            }
        });
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplica() {
        return inBusyLock(() -> {
            TablePartitionId groupId = new TablePartitionId(indexDescriptor.tableId(), 0);

            return placementDriver.awaitPrimaryReplica(groupId, clock.now(), AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC, SECONDS)
                    .handle((replicaMeta, throwable) -> {
                        if (throwable != null) {
                            Throwable cause = unwrapCause(throwable);

                            if (cause instanceof PrimaryReplicaAwaitTimeoutException) {
                                return awaitPrimaryReplica();
                            }

                            return CompletableFuture.<ReplicaMeta>failedFuture(cause);
                        }

                        return completedFuture(replicaMeta);
                    }).thenCompose(Function.identity());
        });
    }

    private CompletableFuture<Void> awaitFinishRwTxsBeforeCatalogVersionOfIndexCreation(LogicalTopologySnapshot logicalTopologySnapshot) {
        return inBusyLock(() -> {
            Set<LogicalNode> remainingNodes = ConcurrentHashMap.newKeySet();
            remainingNodes.addAll(logicalTopologySnapshot.nodes());

            var nodeLeftLogicalTopologyListener = new NodeLeftLogicalTopologyListener(remainingNodes);
            logicalTopologyService.addEventListener(nodeLeftLogicalTopologyListener);

            CompletableFuture[] futures = remainingNodes.stream()
                    .map(node -> awaitFinishRwTxsBeforeCatalogVersionOfIndexCreation(node, remainingNodes))
                    .toArray(CompletableFuture[]::new);

            return allOf(futures)
                    .whenComplete((unused, throwable) -> logicalTopologyService.removeEventListener(nodeLeftLogicalTopologyListener));
        });
    }

    private CompletableFuture<NetworkMessage> awaitFinishRwTxsBeforeCatalogVersionOfIndexCreation(
            LogicalNode targetNode,
            Set<LogicalNode> remainingNodes
    ) {
        return inBusyLock(() -> {
            if (!remainingNodes.contains(targetNode)) {
                // Target node left logical topology, there is no need to do anything.
                return nullCompletedFuture();
            }

            IsNodeFinishedRwTransactionsStartedBeforeRequest request = isNodeFinishedRwTransactionsStartedBeforeRequest();

            return clusterService.messagingService().invoke(targetNode, request, INVOKE_MESSAGE_TIMEOUT_MILLS)
                    .handle((message, throwable) -> {
                        if (throwable != null) {
                            Throwable cause = unwrapCause(throwable);

                            if (!(cause instanceof TimeoutException) && !(cause instanceof RecipientLeftException)) {
                                return CompletableFuture.<NetworkMessage>failedFuture(cause);
                            }
                        } else if (((IsNodeFinishedRwTransactionsStartedBeforeResponse) message).finished()) {
                            remainingNodes.remove(targetNode);

                            return completedFuture(message);
                        }

                        // Let's try to send the message again, but after a short time.
                        return inBusyLock(() -> clockWaiter.waitFor(clock.now().addPhysicalTime(RETRY_SEND_MESSAGE_TIMEOUT_MILLS)))
                                .thenCompose(unused -> awaitFinishRwTxsBeforeCatalogVersionOfIndexCreation(targetNode, remainingNodes));
                    }).thenCompose(Function.identity());
        });
    }

    private CompletableFuture<Void> switchIndexToBuildingStatus() {
        return inBusyLock(() -> catalogManager.execute(StartBuildingIndexCommand.builder().indexId(indexDescriptor.id()).build()));
    }

    private IsNodeFinishedRwTransactionsStartedBeforeRequest isNodeFinishedRwTransactionsStartedBeforeRequest() {
        return FACTORY.isNodeFinishedRwTransactionsStartedBeforeRequest().targetCatalogVersion(catalogVersionOfIndexCreation).build();
    }

    private boolean enterBusy() {
        return IndexManagementUtils.enterBusy(busyLock, taskBusyLock);
    }

    private void leaveBusy() {
        IndexManagementUtils.leaveBusy(busyLock, taskBusyLock);
    }

    private <T> CompletableFuture<T> inBusyLock(Supplier<CompletableFuture<T>> supplier) {
        if (!IndexManagementUtils.enterBusy(busyLock, taskBusyLock)) {
            return failedFuture(new TakStoppingException());
        }

        try {
            return supplier.get();
        } finally {
            IndexManagementUtils.leaveBusy(busyLock, taskBusyLock);
        }
    }
}
