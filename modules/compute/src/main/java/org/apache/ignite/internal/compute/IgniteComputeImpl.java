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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteCompute {
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final Map<ClusterNode, List<RemoteExecutionRequest>> runningJobs = new ConcurrentHashMap<>();

    private final TopologyService topologyService;
    private final IgniteTablesInternal tables;
    private final ComputeComponent computeComponent;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(TopologyService topologyService, IgniteTablesInternal tables, ComputeComponent computeComponent) {
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
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
        ClusterNode candidate = randomNode(candidates);

        candidates.remove(candidate);

        RemoteExecutionRequest req = new RemoteExecutionRequest();
        req.worker = candidate;
        req.units = units;
        req.jobClassName = jobClassName;
        req.args = args;
        req.failoverCandidates = candidates;

        RemoteCompletableFutureWrapper<R> futureWrapper = new RemoteCompletableFutureWrapper<>(
                executeOnOneNode(candidate, units, jobClassName, args));
        req.future = futureWrapper;

        runningJobs.put(candidate, List.of(req));


        var handler = new NodeLeftTopologyEventHandler();
        handler.runningJobs = runningJobs;
        handler.parent = this;
        topologyService.addEventHandler(handler);

        return new JobExecutionWrapper<>(executeOnOneNode(candidate, units, jobClassName, args);
        return futureWrapper;
    }

    private static class RemoteExecutionRequest<T> {
        ClusterNode worker;
        List<DeploymentUnit> units;
        String jobClassName;
        Object[] args;
        Set<ClusterNode> failoverCandidates;
        RemoteCompletableFutureWrapper<T> future;
    }

    public static class NodeLeftTopologyEventHandler implements TopologyEventHandler {
        Map<ClusterNode, List<RemoteExecutionRequest>> runningJobs;
        IgniteComputeImpl parent;

        @Override
        public void onDisappeared(ClusterNode member) {
           runningJobs.get(member).forEach(req -> {
               var futureWrapper = req.future;
               if (req.failoverCandidates.isEmpty()) {
                   futureWrapper.completeExceptionally(new IgniteInternalException("No failover candidates left"));
                   return;
               };

               var remoteFuture = parent.executeOnOneNode(
                           (ClusterNode) req.failoverCandidates.stream().findFirst().get(), req.units, req.jobClassName, req.args
               );

               futureWrapper.setFuture(remoteFuture));
           });
        }
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

    class RemoteCompletableFutureWrapper<T> extends CompletableFuture<T> {
        private CompletableFuture<T> remoteCompletableFuture;

        public RemoteCompletableFutureWrapper(CompletableFuture<T> remoteCompletableFuture) {
            this.remoteCompletableFuture = remoteCompletableFuture;
        }

        public void setFuture(CompletableFuture<T> fut) {
            remoteCompletableFuture = fut;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return remoteCompletableFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return remoteCompletableFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return remoteCompletableFuture.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return remoteCompletableFuture.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return remoteCompletableFuture.get(timeout, unit);
        }
    }

    private <R> JobExecution<R> executeOnOneNode(
            ClusterNode targetNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(units, jobClassName, args);
        } else {
            return computeComponent.executeRemotely(targetNode, units, jobClassName, args);
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyService.localMember());
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return new JobExecutionFutureWrapper<>(requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByTupleKey(table, key))
                .thenApply(primaryNode -> executeOnOneNode(primaryNode, units, jobClassName, args)));
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

        return new JobExecutionFutureWrapper<>(requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByMappedKey(table, key, keyMapper))
                .thenApply(primaryNode -> executeOnOneNode(primaryNode, units, jobClassName, args)));
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

    private static ClusterNode leaderOfTablePartitionByTupleKey(TableViewInternal table, Tuple key) {
        return requiredLeaderByPartition(table, table.partition(key));
    }

    private static  <K> ClusterNode leaderOfTablePartitionByMappedKey(TableViewInternal table, K key, Mapper<K> keyMapper) {
        return requiredLeaderByPartition(table, table.partition(key, keyMapper));
    }

    private static ClusterNode requiredLeaderByPartition(TableViewInternal table, int partitionIndex) {
        ClusterNode leaderNode = table.leaderAssignment(partitionIndex);
        if (leaderNode == null) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, "Leader not found for partition " + partitionIndex);
        }

        return leaderNode;
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
                        node -> new JobExecutionWrapper<>(executeOnOneNode(node, units, jobClassName, args))));
    }
}
