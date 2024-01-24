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

package org.apache.ignite.internal.compute;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteComputeInternal {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteComputeImpl.class);

    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final TopologyService topologyService;

    private final IgniteTablesInternal tables;

    private final ComputeComponent computeComponent;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final LogicalTopologyService logicalTopologyService;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    private final Executor failoverExecutor;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(PlacementDriver placementDriver, TopologyService topologyService,
            LogicalTopologyService logicalTopologyService, IgniteTablesInternal tables, ComputeComponent computeComponent,
            HybridClock clock) {
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.logicalTopologyService = logicalTopologyService;
        this.clock = clock;
        this.failoverExecutor = Executors.newFixedThreadPool(
                1,
                new NamedThreadFactory("compute-job-failover", LOG)
        );
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeAsync(Set<ClusterNode> nodes, List<DeploymentUnit> units, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        Set<ClusterNode> candidates = new HashSet<>(nodes);
        ClusterNode targetNode = randomNode(candidates);
        candidates.remove(targetNode);
        NextWorkerSelector selector = new DeqNexWorkerSelector(new ConcurrentLinkedDeque<>(candidates));

        return new JobExecutionWrapper<>(
                executeOnOneNodeWithFailover(
                        targetNode,
                        selector,
                        units,
                        jobClassName,
                        args
                ));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeAsync(nodes, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = random.nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <R> JobExecution<R> executeOnOneNodeWithFailover(
            ClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(units, jobClassName, args);
        } else {
            return new ComputeJobFailover<R>(
                    computeComponent, logicalTopologyService, topologyService,
                    targetNode, nextWorkerSelector, failoverExecutor, units,
                    jobClassName, args
            ).failSafeExecute();
        }
    }

    private static class DeqNexWorkerSelector implements NextWorkerSelector {
        private final ConcurrentLinkedDeque<ClusterNode> deque;

        private DeqNexWorkerSelector(ConcurrentLinkedDeque<ClusterNode> deque) {
            this.deque = deque;
        }

        @Override
        public CompletableFuture<ClusterNode> next() {
            try {
                return completedFuture(deque.pop());
            } catch (NoSuchElementException ex) {
                return nullCompletedFuture();
            }
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyService.localMember());
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeColocatedAsync(
            String tableName,
            Tuple tuple,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(tuple);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return new JobExecutionFutureWrapper<>(
                requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByTupleKey(table, tuple))
                        .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                                primaryNode,
                                new NextColocatedWorkerSelector<>(tables, placementDriver, topologyService, clock, tableName, tuple),
                                units, jobClassName, args
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> JobExecution<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return new JobExecutionFutureWrapper<>(
                requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByMappedKey(table, key, keyMapper))
                        .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                                primaryNode,
                                new NextColocatedWorkerSelector<>(tables, placementDriver, topologyService, clock, tableName, key, keyMapper),
                                units, jobClassName, args)
                        )
        );
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeColocatedAsync(tableName, key, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<K, R>executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    private CompletableFuture<TableViewInternal> requiredTable(String tableName) {
        String parsedName = IgniteNameUtils.parseSimpleName(tableName);

        return tables.tableViewAsync(parsedName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, parsedName);
                    }
                    return table;
                });
    }

    private CompletableFuture<ClusterNode> primaryReplicaForPartitionByTupleKey(TableViewInternal table, Tuple key) {
        return primaryReplicaForPartition(table, table.partition(key));
    }

    private <K> CompletableFuture<ClusterNode> primaryReplicaForPartitionByMappedKey(TableViewInternal table, K key,
            Mapper<K> keyMapper) {
        return primaryReplicaForPartition(table, table.partition(key, keyMapper));
    }

    private CompletableFuture<ClusterNode> primaryReplicaForPartition(TableViewInternal table, int partitionIndex) {
        TablePartitionId tablePartitionId = new TablePartitionId(table.tableId(), partitionIndex);

        return placementDriver.awaitPrimaryReplica(tablePartitionId, clock.now(), 30, TimeUnit.SECONDS)
                .thenApply(replicaMeta -> {
                    if (replicaMeta != null && replicaMeta.getLeaseholderId() != null) {
                        return topologyService.getById(replicaMeta.getLeaseholderId());
                    }

                    throw new ComputeException(
                            Compute.PRIMARY_REPLICA_RESOLVE_ERR,
                            "Can not find primary replica for [table=" + table.name() + ", partition=" + partitionIndex + "]."
                    );
                });
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, JobExecution<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return nodes.stream()
                .collect(toUnmodifiableMap(identity(),
                        // No failover nodes for broadcast. We use failover here in order to complete futures with exceptions
                        // if worker node has left the cluster.
                        node -> new JobExecutionWrapper<>(executeOnOneNodeWithFailover(node,
                                CompletableFutures::nullCompletedFuture, units, jobClassName, args))));
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync(UUID jobId) {
        return computeComponent.statusAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return computeComponent.cancelAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return computeComponent.changePriorityAsync(jobId, newPriority);
    }
}
