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
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.index.IndexManagementUtils.AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.index.IndexManagementUtils.localNode;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.ChangeIndexStatusValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.message.IndexMessagesFactory;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeRequest;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeResponse;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitException;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatus;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Class representing an abstract task to move an index between different states.
 *
 * <p>Such tasks are intended to be used in cases, when an index cannot be transferred to a new state by simply issuing a corresponding
 * command. These cases include:
 *
 * <ol>
 *     <li>Changing the state of an index from {@link CatalogIndexStatus#REGISTERED} to {@link CatalogIndexStatus#BUILDING};</li>
 *     <li>Changing the state of an index from {@link CatalogIndexStatus#STOPPING} to {@code READ_ONLY} ({@code READ_ONLY} state is
 *     equivalent to the index being removed to the catalog).</li>
 * </ol>
 *
 * <p>Approximate algorithm for a task:
 * <ol>
 *     <li>Wait for the activation (on every cluster node) of the catalog version in which the
 *     {@link CatalogIndexDescriptor#status()} of {@link #targetIndex() index} appeared;</li>
 *     <li>Make sure that the local node is still the primary replica for partition {@code 0} of the table that the index belongs to;</li>
 *     <li>Retrieve the logical topology from the CMG leader;</li>
 *     <li>For each node in the logical topology, send {@link IsNodeFinishedRwTransactionsStartedBeforeRequest} and process the following
 *     results: <ul>
 *         <li>If {@link IsNodeFinishedRwTransactionsStartedBeforeResponse#finished()} is {@code true} - finish request processing;</li>
 *         <li>If {@link IsNodeFinishedRwTransactionsStartedBeforeResponse#finished()} is {@code false} - send the request again after a
 *         short period of time;</li>
 *         <li>If the target node has left the physical topology - send the request again after a short period of time;</li>
 *         <li>If the target node has left the logical topology - finish request processing.</li>
 *     </ul></li>
 *     <li>Execute the Catalog command to change the state of the index.</li>
 * </ol>
 */
abstract class ChangeIndexStatusTask {
    private static final IgniteLogger LOG = Loggers.forClass(ChangeIndexStatusTask.class);

    private static final IndexMessagesFactory FACTORY = new IndexMessagesFactory();

    private static final int INVOKE_MESSAGE_TIMEOUT_MILLS = 1_000;

    private static final int RETRY_SEND_MESSAGE_TIMEOUT_MILLS = 100;

    private final CatalogIndexDescriptor indexDescriptor;

    private final CatalogManager catalogManager;

    private final PlacementDriver placementDriver;

    private final ClusterService clusterService;

    private final LogicalTopologyService logicalTopologyService;

    private final ClockService clockService;

    private final IndexMetaStorage indexMetaStorage;

    private final FailureProcessor failureProcessor;

    private final NodeProperties nodeProperties;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock;

    private final IgniteSpinBusyLock taskBusyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean taskStopGuard = new AtomicBoolean();

    ChangeIndexStatusTask(
            CatalogIndexDescriptor indexDescriptor,
            CatalogManager catalogManager,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            ClockService clockService,
            IndexMetaStorage indexMetaStorage,
            FailureProcessor failureProcessor,
            NodeProperties nodeProperties,
            Executor executor,
            IgniteSpinBusyLock busyLock
    ) {
        this.indexDescriptor = indexDescriptor;
        this.catalogManager = catalogManager;
        this.placementDriver = placementDriver;
        this.clusterService = clusterService;
        this.logicalTopologyService = logicalTopologyService;
        this.clockService = clockService;
        this.indexMetaStorage = indexMetaStorage;
        this.failureProcessor = failureProcessor;
        this.nodeProperties = nodeProperties;
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
            LOG.debug("Starting task to change index status. Index: {}", indexDescriptor);

            return awaitCatalogVersionActivation()
                    .thenComposeAsync(unused -> ensureThatLocalNodeStillPrimaryReplica(), executor)
                    .thenComposeAsync(unused -> inBusyLocks(logicalTopologyService::logicalTopologyOnLeader), executor)
                    .thenComposeAsync(this::awaitFinishRwTxsBeforeCatalogVersion, executor)
                    .thenComposeAsync(unused -> inBusyLocks(() -> catalogManager.execute(switchIndexStatusCommand())), executor)
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            handleStatusSwitchException(throwable);
                        }
                    })
                    .thenApply(unused -> null);
        } finally {
            leaveBusy();
        }
    }

    private void handleStatusSwitchException(Throwable throwable) {
        if (hasCause(
                throwable,
                IndexTaskStoppingException.class,
                NodeStoppingException.class,
                // The index's table might have been dropped while we were waiting for the ability
                // to switch the index status to a new state, so IndexNotFound is not a problem.
                IndexNotFoundValidationException.class,
                // Someone could have already switched the index status, not a problem.
                ChangeIndexStatusValidationException.class,
                // No primary replica is not a reason to fail the node.
                PrimaryReplicaAwaitException.class
        )) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Stop index operation due to an expected exception; index operation is either requested to be stopped "
                                + "or it will be picked up later",
                        throwable
                );
            } else {
                LOG.info(
                        "Stop index operation due to an expected exception; index operation is either requested to be stopped "
                                + "or it will be picked up later [exceptionClass={}, message={}]",
                        throwable.getClass().getName(),
                        throwable.getMessage()
                );
            }
        } else {
            failureProcessor.process(new FailureContext(
                    throwable,
                    String.format("Error starting index task: %s", indexDescriptor.id())
            ));
        }
    }

    /**
     * Returns a CatalogCommand that will be submitted to the Catalog after all RW transactions that had observed the catalog version in
     * which the{@link CatalogIndexDescriptor#status()} of {@link #targetIndex() index} appeared and have finished their execution.
     */
    abstract CatalogCommand switchIndexStatusCommand();

    /** Stops the execution of a task. */
    void stop() {
        if (!taskStopGuard.compareAndSet(false, true)) {
            return;
        }

        taskBusyLock.block();
    }

    CatalogIndexDescriptor targetIndex() {
        return indexDescriptor;
    }

    private CompletableFuture<Void> awaitCatalogVersionActivation() {
        CompletableFuture<HybridTimestamp> activationTimestampFuture = supplyAsync(() -> {
            if (!enterBusy()) {
                throw new IndexTaskStoppingException();
            }

            try {
                return clusterWideEnsuredActivationTimestamp(statusChange().activationTimestamp(), clockService.maxClockSkewMillis());
            } finally {
                leaveBusy();
            }
        }, executor);

        return activationTimestampFuture.thenCompose(clockService::waitFor);
    }

    private CompletableFuture<Void> ensureThatLocalNodeStillPrimaryReplica() {
        return awaitPrimaryReplica().thenAccept(replicaMeta -> {
            if (!enterBusy()) {
                throw new IndexTaskStoppingException();
            }

            try {
                if (!isPrimaryReplica(replicaMeta, localNode(clusterService), clockService.now())) {
                    // Lease has expired, we stop the task.
                    throw new IndexTaskStoppingException();
                }
            } finally {
                leaveBusy();
            }
        });
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplica() {
        return inBusyLocks(() -> {
            IndexMeta indexMeta = indexMetaStorage.indexMeta(indexDescriptor.id());

            if (indexMeta == null) {
                // Index was destroyed under a low watermark, well, we not need to build it.
                throw new IndexTaskStoppingException();
            }

            CatalogTableDescriptor tableDescriptor = catalogManager.catalog(indexMeta.catalogVersion()).table(indexDescriptor.tableId());

            if (tableDescriptor == null) {
                // Table was dropped, no need to build the index.
                throw new IndexTaskStoppingException();
            }

            ZonePartitionId groupId = new ZonePartitionId(tableDescriptor.zoneId(), 0);

            return placementDriver.awaitPrimaryReplica(groupId, clockService.now(), AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC, SECONDS)
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

    private CompletableFuture<Void> awaitFinishRwTxsBeforeCatalogVersion(LogicalTopologySnapshot logicalTopologySnapshot) {
        return inBusyLocks(() -> {
            Set<LogicalNode> remainingNodes = ConcurrentHashMap.newKeySet();
            remainingNodes.addAll(logicalTopologySnapshot.nodes());

            var nodeLeftLogicalTopologyListener = new NodeLeftLogicalTopologyListener(remainingNodes);
            logicalTopologyService.addEventListener(nodeLeftLogicalTopologyListener);

            CompletableFuture<?>[] futures = remainingNodes.stream()
                    .map(node -> awaitFinishRwTxsBeforeCatalogVersion(node, remainingNodes))
                    .toArray(CompletableFuture[]::new);

            return allOf(futures)
                    .whenComplete((unused, throwable) -> logicalTopologyService.removeEventListener(nodeLeftLogicalTopologyListener));
        });
    }

    private CompletableFuture<Void> awaitFinishRwTxsBeforeCatalogVersion(LogicalNode targetNode, Set<LogicalNode> remainingNodes) {
        return inBusyLocks(() -> {
            if (!remainingNodes.contains(targetNode)) {
                // Target node left logical topology, there is no need to do anything.
                return nullCompletedFuture();
            }

            IsNodeFinishedRwTransactionsStartedBeforeRequest request = isNodeFinishedRwTransactionsStartedBeforeRequest();

            return clusterService.messagingService().invoke(targetNode, request, INVOKE_MESSAGE_TIMEOUT_MILLS)
                    .exceptionally(throwable -> {
                        Throwable cause = unwrapCause(throwable);

                        if (!(cause instanceof TimeoutException) && !(cause instanceof RecipientLeftException)) {
                            throw new CompletionException(cause);
                        }

                        return null;
                    })
                    .thenCompose(message -> {
                        if (message != null && ((IsNodeFinishedRwTransactionsStartedBeforeResponse) message).finished()) {
                            LOG.debug("All transactions have finished on node {}. Remaining nodes: {}", targetNode, remainingNodes);

                            remainingNodes.remove(targetNode);

                            return nullCompletedFuture();
                        }

                        // Let's try to send the message again, but after a short time.
                        return inBusyLocks(() -> {
                            Executor delayedExecutor = delayedExecutor(RETRY_SEND_MESSAGE_TIMEOUT_MILLS, MILLISECONDS, executor);

                            return supplyAsync(
                                    () -> awaitFinishRwTxsBeforeCatalogVersion(targetNode, remainingNodes),
                                    delayedExecutor
                            ).thenCompose(Function.identity());
                        });
                    });
        });
    }

    private IsNodeFinishedRwTransactionsStartedBeforeRequest isNodeFinishedRwTransactionsStartedBeforeRequest() {
        return FACTORY.isNodeFinishedRwTransactionsStartedBeforeRequest()
                .targetCatalogVersion(statusChange().catalogVersion())
                .build();
    }

    private boolean enterBusy() {
        return IndexManagementUtils.enterBusy(busyLock, taskBusyLock);
    }

    private void leaveBusy() {
        IndexManagementUtils.leaveBusy(busyLock, taskBusyLock);
    }

    private <T> CompletableFuture<T> inBusyLocks(Supplier<CompletableFuture<T>> supplier) {
        if (!enterBusy()) {
            return failedFuture(new IndexTaskStoppingException());
        }

        try {
            return supplier.get();
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            leaveBusy();
        }
    }

    private MetaIndexStatusChange statusChange() {
        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexDescriptor.id());

        if (indexMeta == null) {
            // Index was destroyed under a low watermark, well, we not need to build it.
            throw new IndexTaskStoppingException();
        }

        MetaIndexStatusChange statusChange = indexMeta.statusChangeNullable(MetaIndexStatus.convert(indexDescriptor.status()));

        assert statusChange != null : IgniteStringFormatter.format("Missing index status change: [indexId={}, catalogStatus={}]",
                indexDescriptor.id(), indexDescriptor.status());

        return statusChange;
    }
}
